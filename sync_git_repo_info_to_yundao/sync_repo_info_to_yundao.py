import MySQLdb
import requests
import pika
import json
import configparser
import sys

class GitlabDataSync:

    # 更新repo_id的值， 参数: appId, repoId
    def update_repo_id(conn, appId, repoId):
        print('更新仓库id...')
        sql = "update application_properties set `value`=%s \
               where application_id=%s \
               and `key`='REPO_ID'" % \
               (str(appId), str(repoId))
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql)
            conn.commit()
            print('更新成功！！！')
        except Exception as e:
            raise Exception("Error: 更新项目(%s)的仓库id为 %s 失败, msg: %s" % (appId, repoId, e))


    # todo 查询gilab项目的路径， 参数appId
    def get_project_path_by_appId(conn, appId):
        # print('get_project_path_by_appId:%s' % appId)
        sql = "SELECT \
                  IF(a.`appset_id` IS NULL, a.`NAME`, CONCAT(`as`.`CODE`, '%2F', a.`NAME`)) \
                FROM application a \
                LEFT JOIN appset `as` ON a.`APPSET_ID`=`as`.`ID` \
                WHERE a.`ID`=" + str(appId)
        
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql)
        except:
            raise Exception("Error: 查询项目(appId=%s)的仓库路径失败" % appId)

        result = cursor.fetchone()

        if result == None:
            raise Exception("Error: 查询项目(appId=%s)的仓库路径失败")
        if len(result)==0:
            raise Exception("Error: 查询项目(appId=%s)的仓库路径失败")
        elif result[0]==None:
            return None
        
        return result[0]

    # 查询所有appId
    def get_all_app_id(conn):
        sql = "SELECT id FROM application a WHERE a.`ENABLED_FLAG`='Y'"
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
        except:
            raise Exception("Error: 查询项目id失败")

        return cursor.fetchall()

    # 创建数据库连接
    def get_db_connection(dbHost, dbPort, dbUser, dbPasswd, dbName):
        conn = MySQLdb.connect(host=dbHost,
                               port=dbPort,
                               user=dbUser,
                               passwd=dbPasswd,
                               db=dbName,
                               charset='utf8')
        
        return conn
    
    
    
    # 查询gilab项目的id， 参数：项目路径， 返回值：项目id
    def find_gitlab_repo_info_by_path(gitlab_url, private_token, projectPath):
        # projectPath=cloudway%2Fundao-deployment
        url = gitlab_url + '/api/v4/projects/'+ projectPath
        # print(url)

        payload = {}
        headers = {
          'Private-Token': private_token,
          'Content-Type': 'application/json'
        }

        try:
            response = requests.request("GET", url, headers=headers, data = payload)
        except:
            print("查询gilab项目异常,msg:", response)

        # print(response.text)
        
        if response.text.find('404') >= 0:
            return None
        else:
            respJson = response.json()
            return {'id':respJson.get('id'), 'repo_url':respJson.get('ssh_url_to_repo')}


    # 登录云道， 参数：username, password, 返回值： token
    def login_yundao(yundaoAddr, login_url, username, password):
        url = yundaoAddr + login_url + '?username=%s&password=%s' % (username, password);
        # print('login url:%s' % url)
        payload = {}
        headers = {
          'Content-Type': 'application/json'
        }

        try:
            response = requests.request("POST", url, headers=headers, data = payload)
        except:
            raise Exception("登录失败,msg:%s" % response)

        respJson = response.json()
        if respJson.get('success') == 'false':
            raise Exception("登录失败,msg:%s" % response)

        return respJson.get('data').get('token')
        
        
    
    # 获取rabbitmq连接通道
    def get_rabbitmq_channel(ip, port, username, password, vhost, queue):
        credentials = pika.PlainCredentials(username, password)
        connParams = pika.ConnectionParameters(ip, port, vhost, credentials)
        connection = pika.BlockingConnection(connParams)

        channel = connection.channel();
        channel.queue_declare(queue=queue, durable=True)
        
        return channel
        
    

    # 发送消息，通知构建队列，完成构建任务的更新操作， 参数： token, appId
    def send_msg_to_update_repo_addr_in_build_file(mqChannel, token, appId, repoAddr):

        properties = pika.BasicProperties(content_type='application/json', 
            content_encoding='UTF-8',
            delivery_mode=1)

        msg = {
                'header':{'ACTION_TYPE':'APPLICATION_APPSET_UPDATE', 'Authorization':'Basic %s' % token},
                'body':{'appId':appId, 'repoAddr':repoAddr}
            }
        
        # print('msg: %s' % json.dumps(msg))
        try:
            mqChannel.basic_publish(exchange='',
                                    routing_key='BUILD_QUEUE',
                                    body=json.dumps(msg),
                                    properties=properties)
        except Exception as e:
            raise Exception("消息发送失败,msg:%s" % e)
        
    def parse_str_to_int_list(string, seperator):
        if len(string) == 0:
            return []

        ids = []
        strList = string.replace('[', '').replace(']', '').replace(' ', '').split(seperator)
        for i in strList:
            if i != '':
                ids.append(int(i))
                
        return ids



    # 主函数，程序入口
    if __name__ == '__main__':

        
        # 载入配置
        cf = configparser.ConfigParser()
        cf.read('./config.ini')

        # 获取数据库连接和MQ通道
        db_conn = get_db_connection(cf.get('MYSQL', 'host'),
                                    int(cf.get('MYSQL', 'port')),
                                    cf.get('MYSQL', 'username'),
                                    cf.get('MYSQL', 'password'),
                                    cf.get('MYSQL', 'db'))
        
        mq_channel = get_rabbitmq_channel(cf.get('MQ', 'host'),
                                          int(cf.get('MQ', 'port')),
                                          cf.get('MQ', 'username'),
                                          cf.get('MQ', 'password'),
                                          cf.get('MQ', 'vhost'),
                                          'BUILD_QUEUE')

        # 获取appIds
        isAll = False
        
        appIds = []
        inclusive = parse_str_to_int_list(cf.get('SYNC_APPS', 'inclusive'), ',')
        exclusive = parse_str_to_int_list(cf.get('SYNC_APPS', 'exclusive'), ',')
        if len(inclusive) == 0:
            isAll = True
            inclusive = get_all_app_id(db_conn)

        if len(inclusive) == 0:
            print('no app found in db, no need to sync')
            quit()

        if len(exclusive) > 0:
            for i in inclusive:
                if exclusive.count(i) == 0:
                    appIds.append(i)
                else:
                    continue
        else:
            appIds = inclusive


        confirm_msg = '您将要同步的应用为：'
        if isAll:
            confirm_msg = confirm_msg + '【所有应用】'
            if len(exclusive)>0:
                confirm_msg = confirm_msg + '，但不包含' + cf.get('SYNC_APPS', 'exclusive')
            else:
                pass
        else:
            confirm_msg = confirm_msg + cf.get('SYNC_APPS', 'inclusive')
            if len(exclusive)>0:
                confirm_msg = confirm_msg + ', 但不包含' + cf.get('SYNC_APPS', 'exclusive')
        
        print(confirm_msg)
        print('请确认[y/n]：')
        confirm = sys.stdin.readline().strip()
        if confirm == 'y':
            print('开始同步...')
        else:
            quit()


        # 遍历所有appId， 执行数据同步的逻辑
        for appId in appIds:
            print('==================== sync app(appId=%s)======================' % appId)
            continue
            try:
                #main(cf, db_conn, mq_channel, appId)
                
                # 获取项目路径
                project_path = get_project_path_by_appId(db_conn, appId)
                print('项目路径: %s' % project_path)
                if project_path == None:
                    raise Exception('查询项目(appId=%s)的gitlab路径失败' % appId)


                # 获取仓库信息
                if project_path.find('%2F') < 0:
                    project_path = cf.get('GITLAB', 'default_user') + '%2F' + project_path
                    repo_info = find_gitlab_repo_info_by_path(cf.get('GITLAB', 'url'),
                                                  cf.get('GITLAB', 'private_token'),
                                                  project_path)
                    if repo_info == None:
                        project_path = cf.get('GITLAB', 'default_group') + '%2F' + project_path.split('%2F')[1]
                        repo_info = find_gitlab_repo_info_by_path(cf.get('GITLAB', 'url'),
                                                  cf.get('GITLAB', 'private_token'),
                                                  project_path)
                else:
                    repo_info = find_gitlab_repo_info_by_path(cf.get('GITLAB', 'url'),
                                                  cf.get('GITLAB', 'private_token'),
                                                  project_path)
                
                if repo_info == None:
                    raise Exception('查询仓库(%s)失败' % project_path)
                
                print('仓库id=%s, 仓库url=%s' % (repo_info.get('id'), repo_info.get('repo_url')))
        
                # 获取token
                token = login_yundao(cf.get('YUNDAO', 'yundao_url'),
                             cf.get('YUNDAO', 'login_url'),
                             cf.get('YUNDAO', 'username'),
                             cf.get('YUNDAO', 'password'))
                # print('云道token: %s' % token)

                # 更新云道项目与gilab项目的关联关系
                update_repo_id(db_conn, appId, repo_info.get('id'))

                # 发送消息，更新build file
                print('发送消息,更新构建任务配置文件...')
                send_msg_to_update_repo_addr_in_build_file(mq_channel,
                                                   token,
                                                   appId,
                                                   repo_info.get('repo_url'))
                print('发送成功')

                print('\n')
            except Exception as e:
                print("Exception:", e)
                print('是否继续 ? [y/n]')
                decision = sys.stdin.readline().strip()
                if decision != 'y':
                    print('即将结束并退出...')
                    db_conn.close()
                    mq_channel.close()
                    print('已结束.')
                    break
                
        print('同步结束.')
                
    
    
    
