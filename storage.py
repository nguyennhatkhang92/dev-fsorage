import time, datetime, re
from threading import Thread, Condition
from backend.models import SystemFile, UserFile
from backend.exts import mongo, sql, redis, cache
from backend.components import AriaRPC
from backend.utils import try_connect_mysql, json, filename_shortcut, console_text, write_pid, update_pid_expire
from flask.ext.script import Command, Option
from . import manager
from flask import current_app
from sqlalchemy.sql import func
from sqlalchemy.exc import OperationalError
from pymongo.errors import PyMongoError, OperationFailure
from binascii import hexlify

@manager.option('--source', '-s', dest='src', default=[])
@manager.option('--dest', '-d', dest='dst', default=[])
@manager.option('--day','-da', dest='day', default=0)
@manager.option('--limit','-l', dest='limit', default=0)
def insert_migrate(src, dst, day, limit):
    def use():
        print 'Command get_migrating usage'
        print 'get_migrating -s/--source [x,x,x]/x,x,x -d/--dest [y,y,y]/y,y,y -da/--day n -l/--limit z'
        print 'Example: '
        print '1. get_migrating -s [1,2,3] -d [4,5,6] -da 1 -l 10'
        print '2. get_migrating -s 1,2,3 -d 4,5,6 -da 1 -l 10'
        print '3. get_migrating -s 1 -d 2 -da 1 -l 10'

    if not src or not dst or not limit or not day:
        use()
        return
    patt = re.compile(r'\[(\d+,?)+\]|\d+,?|\d+')
    if not patt.match(src) or not patt.match(dst):
        use()
        return
    try:
        limit = int(limit)
        day = int(day)
    except:
        use()
        return
    src = re.findall('\d+', src)
    dst = re.findall('\d+', dst)

    ost_fid = {}
    count = 0
    src_files = {}

    sysFile = sql.session.query(SystemFile.id, SystemFile.storage_id).join(UserFile, SystemFile.id==UserFile.pid)

    for ost in src:
        ost = int(ost)
        src_files[ost] = sysFile.filter(SystemFile.storage_id == ost, SystemFile.count > 0,
            UserFile.lastdownload < func.unix_timestamp(datetime.datetime.now() - datetime.timedelta(days=day))) \
            .order_by(UserFile.size.desc()).limit(limit).all()

    while limit != 0:
        for ost in src_files.keys():

            if ost not in ost_fid.keys():
                ost_fid[ost] = 0

            if len(src_files[ost]) == 0:
                del src_files[ost]
                del ost_fid[ost]
                continue
            try:
                mongo.Migration.collection.insert({
                    '_id': src_files[ost][ost_fid[ost]][0] ,
                    'src_id': ost,
                    'dst_id': int(dst[count]),
                    'stat': 'QUEUE',
                    })
            except PyMongoError as e:
                ost_fid[ost] += 1
                if len(src_files[ost]) == ost_fid[ost]:
                    del ost_fid[ost]
                    del src_files[ost]
                continue

            ost_fid[ost] += 1

            if len(src_files[ost]) == ost_fid[ost]:
                del ost_fid[ost]
                del src_files[ost]

            count += 1
            if len(dst) == count:
                count = 0
        limit = limit -1

@manager.option('--type', '-t', dest='type', default = 'dst')
def list_migrate(type):
    """ Show list files in migrations collections """

    def use():
        print 'Use command list_migrate:'
        print '  1. list_migrate --type/-t src/dst'
        print '  2. list_migrate --type/-t s/d'
        print '  *Note: Default option type is "src_id"'

    if type not in ['src', 'dst', 's', 'd']:
        use()
        return

    if type in ('src', 's'):
       type = 'src_id'

    elif type in ('dst', 'd'):
       type = 'dst_id'

    _  = console_text
    migrating = mongo.Migration.collection.aggregate([
        {'$group': {
            '_id'   : {type: '$%s' % type, 'stat': '$stat'},
            'count' : { '$sum' : 1 },
            'sid'   : {'$addToSet': '$_id'}
        }}
        ])['result']

    show = {}
    move = []

    for m in migrating:
        stat = m['_id']['stat']
        t = m['_id'][type]

        if t not in show:
            show[t] = {'queue': 0,'migrate': [], 'error': 0, 'total': 0 }

        if 'QUEUE' in m['_id'].values():
            show[t]['queue'] = m['count']
        elif 'MIGRATING' in m['_id'].values():
            show[t]['migrate'] = m['sid']
        else:
            show[t]['error'] += m['count']
        show[t]['total'] += m['count']

    print _('  FILTER_BY/%-3s       TOTAL_FILES      QUEUE_FILES       ERROR_FILES       FILES_MIGRATING  '%type.upper() ,32,1)
    for k, v in show.items():
        print '  %-22d %-16d %-17d %-17d %-50s' % (k, v['total'], v['queue'], v['error'], len(v['migrate']))
        for i in v['migrate']:
            move.append(i)

    print '\r\n'
    print _('  LIST FILES MIGRATING:', 32, 1)
    print _('  FID            SOURCE       DEST       GID  ', 32)
    for i in move:
        ret = mongo.Migration.collection.find({'_id': i })
        for f in ret:
            print '  %-14d %-12d %-10d %-16s  ' % (i, f['src_id'], f['dst_id'], f['gid'])

def wait_return_salt_cmd(jid, timeout, interval = 5):
    from salt.client import LocalClient

    client = LocalClient()
    result = {}
    start = time.time()
    while True:
        result = client.get_cache_returns(jid)
        if len(result) == 0:
            wait = int(time.time() - start)
            if wait > timeout:
                return {}
        else:
            return result
        time.sleep(interval)

@cache.memoize(timeout=60*60)
def cache_ost_grains_item(ost, args=['default_addr'], timeout=60):
    from salt.client import LocalClient
    client = LocalClient()
    ret = {}
    try:
        ost = int(ost)
    except:
        return False
    if not isinstance(args, list):
        return False
    jid = client.cmd_async('G@osts:%d'% ost, 'grains.item', args, expr_form='compound')
    ret = wait_return_salt_cmd(jid, timeout=timeout)
    return ret

@manager.command
def move_files():
    '''Migrate file in Migration collection'''
    from salt.client import LocalClient
    cv = Condition()
    app = mongo.app
    tmp_dst_path = '/var/www/STO-%d/temp'
    dst_path     = '/var/www/STO-%d/FshareFS/mega/%s/%s'
    client = LocalClient()
    aria = None
    migratings = {}
    files_moving = {}
    resum = {}

    def res_grains_to_dict(salt_res):
        ret = {}
        for i in salt_res:
            osts = salt_res[i]['ret']['osts']
            if len(osts) > 1:
                for ost in osts:
                    ret[ost] = [i, salt_res[i]['ret']['default_addr']]
                continue
            ret[osts[0]]=[i, salt_res[i]['ret']['default_addr']]
        return ret

    def check_file_done(dst_id, file_id, file_path):
        sto_id = None
        new_file_name = None
        folder_path = None
        src_move = None
        dst_move = None
        minion_id = None
        with app.test_request_context():
            res = cache_ost_grains_item(dst_id, args=['default_addr', 'osts'])
            if not res or len(res) == 0 or type(res[res.keys()[0]]['ret'])=='str':
                return
            minion_id = res_grains_to_dict(res)
            minion_id = minion_id[dst_id][0]
        file_name = file_path.rsplit('/',1)[1]
        with app.test_request_context():
            try_connect_mysql()
            sysFile   = SystemFile.query.filter_by(id = file_id).first()
            # Record in DB is deleted before moving file
            if sysFile == None:
                client.cmd(minion_id, 'storage.delete_files', [dst_id] + [file_path], timeout = 60*10, expr_form='glob')
                mongo.Migration.collection.update({'_id': file_id}, {'$set': {'stat': "REMOVE_BEFORE"}})
                return

            folder_path = sysFile.folder_path
            sub_folder = folder_path.split('/')
            if len(sub_folder) == 1 and len(folder_path) > 6:
                folder_path = folder_path[:6] + '/' + folder_path[6:]

            src_move  = '%s/%s' % (tmp_dst_path % dst_id, file_name)
            if file_name.startswith('migrate_'):
                file_name = file_name[8:]
            dst_move  = dst_path % (dst_id, folder_path, file_name)
        while True:
            jid = client.cmd_async(minion_id, 'storage.move_file', [src_move, dst_move, 400, dst_id], expr_form='glob')
            ret = wait_return_salt_cmd(jid, timeout=60*10)
            if len(ret) == 0:
                continue

            new_file_name = ret[ret.keys()[0]]['ret']
            with app.test_request_context():
                if ' ' in new_file_name: # filename not have space
                    mongo.Migration.collection.update({'_id': file_id}, {'$set': {'stat': "MOVE_ERR"}})
                    return

                try_connect_mysql()
                sysFile   = SystemFile.query.filter_by(id = file_id).first()
                ost = dst_id
                sto_id = sysFile.storage_id
                file_path = dst_path % (sysFile.storage_id, sysFile.folder_path, sysFile.name)

                if sysFile == None:# Record in DB is deleted after moving file
                    new_file_path = dst_path % (ost, folder_path, new_file_name)
                    client.cmd(minion_id, 'storage.delete_files', [ost] + [new_file_path],
                        timeout=60*10, expr_form='glob')
                    mongo.Migration.collection.update({'_id': sid}, {'$set': {'stat': "REMOVE_AFTER"}})
                    return

            with app.test_request_context():
                # Update DB
                count = 3
                try_connect_mysql() # Update record in DB
                while count > 0:
                    try:
                        sess = sql.session()
                        sess.query(SystemFile).filter(SystemFile.id == file_id).update({"storage_id": long(dst_id),
                            "name": new_file_name, "folder_path": folder_path })
                        sess.commit()
                        break
                    except OperationalError as e:
                        if '1205' in e[0]:
                            time.sleep(1)
                            count -= 1
                            if count == 0:
                                return
                        else:
                            sess.rollback()
                            raise e

                mongo.FileRemoveSchedule.collection.insert({
                    'file_path'     : file_path,
                    'ost'           : sto_id,
                    'schedule_time' : datetime.datetime.utcnow() + datetime.timedelta(hours=24),
                    })
                mongo.Migration.collection.remove({'_id': file_id})
            break

    def get_result(migratings):
        while True:
            wait = 60
            for dst_id in migratings.keys():
                ret = {}
                with app.test_request_context():
                    res = cache_ost_grains_item(dst_id, args=['default_addr', 'osts'])
                    if not res or len(res) == 0 or type(res[res.keys()[0]]['ret']) == 'str':
                        continue
                    ret = res_grains_to_dict(res)
                jid = client.cmd_async(ret[dst_id][0], 'aria2.tellStatus', [migratings[dst_id][0]], expr_form='glob')
                aria = wait_return_salt_cmd(jid, timeout=20)
                if len(aria) == 0 or not isinstance(aria[aria.keys()[0]]['ret'], dict):
                    with app.test_request_context():
                        current_app.logger.error('salt %s aria2.tellStatus(%s) not return (jid: %s)',
                        migratings[dst_id][2], migratings[dst_id][0], jid)
                    continue
                stat = aria[aria.keys()[0]]['ret']
                if stat['status'] == 'complete':
                    check_file_done(dst_id, migratings[dst_id][1], stat['files'][0]['path'])
                    # Delete cac system file id da download xong/error ra de add system file id khac vao download tiep
                    del migratings[dst_id]
                    # remove result
                    client.cmd_async(aria.keys()[0], 'aria2.removeDownloadResult', [stat['gid']])
                    cv.acquire()
                    cv.notify()
                    cv.release()
                elif stat['status'] == 'error':
                    # Update stat -> ERROR & gid in mongo
                    with app.test_request_context():
                        mongo.Migration.collection.update({'_id': migratings[dst_id][1]} , {'$set': {'stat': "ARIA_ERROR_" + stat['errorCode']}})
                        current_app.logger.error('gid %s error %s: fid %d', stat['gid'], stat['errorCode'], migratings[dst_id][1])
                    # Delete cac system file id da download xong/error ra de add system file id khac vao download tiep
                    del migratings[dst_id]
                    cv.acquire()
                    cv.notify()
                    cv.release()
                else:
                    # Maximum time waiting is 60s
                    if (long(stat['downloadSpeed']) == 0):
                        wait = 60
                    else:
                        wait = (long(stat['files'][0]['length']) - long(stat['files'][0]['completedLength'])) / long(stat['downloadSpeed'])
                        wait = min(wait, 60)
            if wait < 10:
                wait = 10
            time.sleep(wait)

    if not write_pid('move_files', 30*60):
        return

    th = Thread(target = get_result, args = (migratings,))
    th.daemon = True
    th.start()

    count = 3
    while count > 0:
        try:
            ret = mongo.Migration.find({'stat': "MIGRATING"})
            for m in ret:
                migratings[m['dst_id']] = [m['gid'], m['_id']]
            break
        except OperationFailure as e:
            patt = re.compile(r"cursor id '(\d+)' not valid at server")
            if patt.match(e.message):
                time.sleep(1)
                count -= 1
                if count == 0:
                    return
            else:
                raise e
    while True:
        if not th.isAlive():
            return
        count = 3
        while count > 0:
            try:
                list_queue = mongo.Migration.find({'stat': "QUEUE"})
                for m in list_queue:
                    if m['dst_id'] in migratings:
                        continue

                    try_connect_mysql()
                    q = sql.session.query(SystemFile.name, SystemFile._checksum, SystemFile.storage_id, SystemFile.folder_path, UserFile.name). \
                        join(UserFile, SystemFile.id==UserFile.pid).filter(SystemFile.id == m['_id']).first()
                    # Record in DB is deleted before move file from source to dest's temp dir
                    if q == None or q[2] != m['src_id']:
                        mongo.Migration.collection.remove({'_id': m['_id']})
                        continue

                    src_id = cache_ost_grains_item(m['src_id'], args=['default_addr', 'osts'])
                    dst_id = cache_ost_grains_item(m['dst_id'], args=['default_addr', 'osts'])
                    if (not src_id or len(src_id) == 0 or type(src_id[src_id.keys()[0]]['ret']) == 'str') or \
                       (not dst_id or len(dst_id) == 0 or type(dst_id[dst_id.keys()[0]]['ret']) == 'str'):
                        continue
                    src_id = res_grains_to_dict(src_id)
                    dst_id = res_grains_to_dict(dst_id)

                    url = 'http://%s:89/private/STO-%d/FshareFS/mega/%s/%s' % (src_id[m['src_id']][1] , m['src_id'], q[3], q[0])
                    checksum = hexlify(q[1])
                    file_name = q[4]
                    file_name = filename_shortcut(file_name, lenght = 70, lower = True)
                    if file_name.startswith('_'):
                        file_name = checksum + file_name
                    else:
                        file_name = checksum + '_'+ file_name
                    file_name = 'migrate_' + file_name
                    ret = client.cmd(dst_id[m['dst_id']][0], 'aria2.addUri',
                            [url, 'dir=%s' % tmp_dst_path % m['dst_id'], 'out=%s' % file_name],
                            timeout = 60, expr_form='glob')

                    if len(ret) == 0 or len(ret[ret.keys()[0]]) > 16 or len(ret[ret.keys()[0]]) == 0:
                        continue
                    gid = ret[ret.keys()[0]]
                    mongo.Migration.collection.update({'_id': m['_id']}, {'$set': {'stat': "MIGRATING", 'gid': gid}})
                    migratings[m['dst_id']] = [gid, m['_id']]
                break

            except OperationFailure as e:
                patt = re.compile(r"cursor id '(\d+)' not valid at server")
                if patt.match(e.message):
                    time.sleep(1)
                    count -= 1
                    if count == 0:
                        return
                else:
                    raise e

        update_pid_expire('move_files', 30*60)
        cv.acquire()
        cv.wait(60*2)
        cv.release()

@manager.command
def clean_orphan_files_schedule():
    """ Clean orphan systemfile in DB """

    if not write_pid('clean_orphan_files_schedule'):
        return

    count = 0
    dst_path     = '/var/www/STO-%d/FshareFS/mega/%s/%s'
    # Only delete file after file is modified over ``FILE_TIME_DELETE`` days
    dt = time.mktime((datetime.datetime.now() - datetime.timedelta(days=current_app.config['FILE_TIME_DELETE'])).timetuple())
    try_connect_mysql()
    del_files = SystemFile.query.filter(SystemFile.count==0,SystemFile.modified<dt)
    s = sql.session() # create session

    for sysFile in del_files:
        mongo.FileRemoveSchedule.collection.insert({
            'file_path'     : dst_path % (sysFile.storage_id, sysFile.folder_path, sysFile.name),
            'ost'           : sysFile.storage_id,
            'schedule_time' : datetime.datetime.utcnow(),
        })
        s.query(SystemFile).filter_by(id = sysFile.id).delete(synchronize_session=False)
        count += 1
        if count == 5:
            s.commit()
            count = 0

    s.commit()

@manager.command
def delete_schedule():
    """ Delete physical file """

    from salt.client import LocalClient

    client = LocalClient()

    def get_result(deletings):

        while True:
            for ost in deletings.keys():
                result = client.get_cache_returns(deletings[ost][0])

                if len(result) == 0 or len(result[result.keys()[0]]['ret']) == 0:
                    wait = int(time.time() - deletings[ost][2])
                    if wait > 900 :
                        del deletings[ost]
                    else:
                        continue

                else:
                    ret = result[result.keys()[0]]['ret']
                    if type(ret) == str:
                        with mongo.app.test_request_context():
                            current_app.logger.fatal(ret)
                            print ost, ret
                    else:
                        for f in deletings[ost][1]:
                            with mongo.app.test_request_context():
                                mongo.FileRemoveSchedule.collection.remove({'_id': f._id})

                    del deletings[ost]

            time.sleep(0.3)

    if not write_pid('delete_schedule'):
        return

    system_files = {} # Cac system file can xoa trong DB
    deletings = {} # Cac file vat ly can xoa
    del_file = {}

    th = Thread(target = get_result, args = (deletings,))
    th.daemon = True
    th.start()

    while True:
        remove_schedule_files = mongo.FileRemoveSchedule.find({'schedule_time': {'$lte': datetime.datetime.utcnow()}}).limit(5000)
        if remove_schedule_files.count() == 0: break
        for f in remove_schedule_files:
            if f.ost in system_files:
                system_files[f.ost].append(f)
            else:
                system_files[f.ost] = [f]

        while len(system_files) > 0:
            for ost in system_files:
                if ost in deletings:
                    continue

                paths = []
                del_file[ost] = system_files[ost][:5]

                for f in del_file[ost]:
                    paths.append(f.file_path)

                jid = client.cmd_async('osts:%s' % ost, 'storage.delete_files', [ost] + paths, expr_form='grain')
                if jid:
                    deletings[ost] = [jid, del_file[ost], time.time()]
                else:
                    current_app.logger.error("Can't run delete_files for ost %s", ost)
                system_files[ost] = system_files[ost][5:]

            for ost in system_files.keys():
                if len(system_files[ost]) == 0:
                    del system_files[ost]

            time.sleep(30)
