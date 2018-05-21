from write_mysql import *


class Adc(Write):
    # 继承类(返回删除字典、插入字典)
    def process_data(self, hb_dict):
        dct_dict = {}
        ins_dict = {}
        dct_dict['account_id'] = hb_dict['account_id']
        dct_dict['idea_id'] = hb_dict['cpcIdeaId']
        dct_dict['adgroup_id'] = hb_dict['cpcGrpId']

        ins_dict['account_id'] = hb_dict['account_id']
        ins_dict['idea_id'] = hb_dict['cpcIdeaId']
        ins_dict['adgroup_id'] = hb_dict['cpcGrpId']
        ins_dict['idea_name'] = hb_dict['title']
        ins_dict['idea_status'] = 0
        ins_dict['json_idea'] = json.dumps(hb_dict)
        ins_dict['created_at'] = Utils.get_today_second()
        ins_dict['updated_at'] = Utils.get_today_second()

        process_dict = (dct_dict, ins_dict)
        return process_dict


if __name__ == '__main__':
    adv = Adc()
    start_date, end_date = Utils.get_date_by_input_params(sys.argv[1:])
    adv.start_date = start_date
    adv.end_date = end_date
    adv.table_name = 'sogou_creatives'
    adv.mysql_table = 'sogou_idea'
    adv.mysql_num = 100
    adv.batch_size = 10
    adv.start()
