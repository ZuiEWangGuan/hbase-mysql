import pymysql
import getopt
import sys
import logging
import time
import json
import re
import happybase
from datetime import timedelta, datetime
from readconfig import Read


class Write:

    def __init__(self):
        # mysql连接
        self.mysql = self.get_mysql_conn()
        # 插入语句
        self.insert_sql = "insert into {}({}) values({})"
        # 删除语句
        self.delete_sql = "DELETE FROM {} WHERE {}"
        # hbase表名
        self.table_name = ''
        # 开始时间 (包含)
        self.start_date = ''
        # 结束时间 (不包含)
        self.end_date = ''
        # 从hbase 每一次scan数量
        self.batch_size = 1000
        # 每次插入mysql 的数量
        self.mysql_num = 1000
        # mysql 的表名
        self.mysql_table = ''
        self.ins_list = []
        self.dct_list = []

    # mysql连接
    @staticmethod
    def get_mysql_conn():
        config = {
            'host': Read.mysql_log_host,
            'user': Read.mysql_log_user,
            'passwd': Read.mysql_log_passwd,
            'db': Read.mysql_log_db,
            # self.mysql_write_host = Read.mysql_write_host
            # self.mysql_write_port = Read.mysql_write_port
            # self.mysql_write_user = Read.mysql_write_user
            # self.mysql_write_passwd = Read.mysql_write_passwd
            # self.mysql_write_db = Read.mysql_write_db
            # 'host': self.mysql_write_host,
            # 'user': self.mysql_write_user,
            # 'passwd': self.mysql_write_passwd,
            # 'db': self.mysql_write_db,
            'charset': 'utf8'
        }
        db = pymysql.connect(**config)
        return db

    # hbase 连接
    @staticmethod
    def get_hb_conn():
        hbase_pool = happybase.ConnectionPool(size=2, host=Read().hbase_host)
        return hbase_pool

    # 从hbase scan数据
    def write_data_mysql(self):
        with self.get_hb_conn().connection() as connection:
            table = connection.table(self.table_name)
            try:
                for i in range(0, 10):
                    scan_hb_data = table.scan(row_start='{}|{}'.format(i, self.start_date),
                                              row_stop='{}|{}'.format(i, self.end_date), limit=100)
                    for hb_key, hb_value in scan_hb_data:
                        for value in hb_value.items():
                            result = re.sub('\'', '\"', value[1].decode())
                            hb_dict = json.loads(result)
                            process_dict = self.process_data(hb_dict)
                            self.put_data_mysql(process_dict)
            except Exception as e:
                logging.error('{}'.format(repr(e)))
                self.write_data_mysql()
        self.execute_sql()

    # 满足一定条数写入到mysql
    def put_data_mysql(self, process_dict):
        dct_dict = process_dict[0]
        ins_dict = process_dict[1]
        dct_list = []
        ins_list = []
        for dct_value in dct_dict.values():
            dct_list.append(dct_value)
        for ins_value in ins_dict.values():
            ins_list.append(ins_value)
        self.dct_list.append(dct_list)
        self.ins_list.append(ins_list)
        self.set_sql(dct_dict, ins_dict)
        if len(self.ins_list) == self.mysql_num:
            self.execute_sql()
            self.dct_list.clear()
            self.ins_list.clear()

    # 设置mysql的删除和插入语句
    def set_sql(self, dct_dict, ins_dict):
        if '{}' in self.insert_sql:
            dct_tem = ''
            ins_tem1 = ''
            ins_tem2 = ''
            for dct_key in dct_dict.keys():
                dct_tem += 'AND {} = %s '.format(dct_key)
            for ins_key in ins_dict.keys():
                ins_tem1 += ',{}'.format(ins_key)
                ins_tem2 += ',{}'.format('%s')
            self.insert_sql = self.insert_sql.format(self.mysql_table, ins_tem1[1:], ins_tem2[1:])
            self.delete_sql = self.delete_sql.format(self.mysql_table, dct_tem[4:])

    # 需要继承,返回一个包含删除和插入数据的元组
    def process_data(self, hb_dict):
        return hb_dict

    # 执行sql
    def execute_sql(self):
        with self.mysql as db:
            try:
                db.executemany(self.delete_sql, self.dct_list)
                db.executemany(self.insert_sql, self.ins_list)
            except Exception as e:
                logging.error(repr(e))
                self.mysql.rollback()

    # 开始执行任务
    def start(self):
        self.write_data_mysql()


class Utils:

    # 解析外部传入参数，未传入则查前一天，传入则查指定日期范围，异常则提示并退出
    @staticmethod
    def get_date_by_input_params(argv):
        if len(argv) > 0:
            start_date = ''
            end_date = ''
            tag_words = ' -s <write mysql start date: "yyyyMMdd">' \
                        ' -e <write mysql end date: "yyyyMMdd">'
            try:
                opts, args = getopt.getopt(argv, "hs:e:", ["start_date=", "end_date="])
            except getopt.GetoptError:
                sys.exit()
            for opt, arg in opts:
                if opt == '-h':
                    logging.info(tag_words)
                    sys.exit()
                elif opt in ("-s", "--start_date"):
                    start_date = arg
                elif opt in ("-e", "--end_date"):
                    end_date = arg
                else:
                    logging.info(tag_words)
                    sys.exit()
            logging.info('write mysql start date: {}, write mysql end date: {}'.format(start_date, end_date))
            date_range = (start_date, end_date)
            return date_range
        else:
            logging.info('report start date: {}, report end date: {}'.format(Utils.get_yesterday(), Utils.get_today()))
            date_range = (Utils.get_yesterday(), Utils.get_today())
            return date_range

    # 获取前一天日期格式---20180519
    @staticmethod
    def get_yesterday():
        yesterday = datetime.today() + timedelta(-1)
        return yesterday.strftime("%Y%m%d")

    # 获取前一天日期格式---2018-05-19
    @staticmethod
    def get_yesterday_by_point():
        yesterday = datetime.today() + timedelta(-1)
        return yesterday.strftime("%Y-%m-%d")

    # 获取今天日期格式---20180520
    @staticmethod
    def get_today():
        today = time.strftime("%Y%m%d")
        return today

    # 获取今天日期格式---20180520
    @staticmethod
    def get_today_second():
        today_hour = time.strftime("%Y-%m-%d %H:%M:%S")
        return today_hour
