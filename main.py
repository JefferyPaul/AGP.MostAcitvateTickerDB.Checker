"""

"""
import os
import shutil
from datetime import datetime, date, timedelta
import json
from typing import List, Dict
import sys
from collections import defaultdict
from pprint import pprint
import argparse

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)
PATH_CONFIG = os.path.join(PATH_ROOT, 'Config', 'Config.json')

from pyptools.helper.simpleLogger import MyLogger
from pyptools.helper.tp_WarningBoard import run_warning_board
from pyptools.MostActivateTickerDB import MostActivateTickerToDB, MostActivateTicker, MostActivateTickerFile, MostActivateTickerFileData


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-o', '--output')
arg_parser.add_argument('--otoday', help='是否输出当天的数据', action='store_true')
arg_parser.add_argument('-d', '--dayoffset', help='开始日期(距离当前的天数).当天:0,昨天:-1.', default=0)
args = arg_parser.parse_args()
PATH_OUTPUT = os.path.abspath(args.output)
IS_OUTPUT_TODAY = args.otoday
START_DAY_OFFSET = int(args.dayoffset)
if START_DAY_OFFSET > 0:
    START_DAY_OFFSET = -START_DAY_OFFSET
if not os.path.isdir(PATH_OUTPUT):
    os.makedirs(PATH_OUTPUT)


# 合成得到“远月"合约
def gen_longer(
        data1: List[MostActivateTickerFileData],
        data2: List[MostActivateTickerFileData]) -> List[MostActivateTickerFileData]:
    d_longer = defaultdict(dict)
    for _data_list in [data1, data2]:
        if not _data_list:
            continue
        for _data in _data_list:
            if not d_longer.get(_data.product):
                d_longer[_data.product][_data.date] = _data
            elif not d_longer[_data.product].get(_data.date):
                d_longer[_data.product][_data.date] = _data
            else:
                _ticker_1 = d_longer[_data.product][_data.date].ticker
                _ticker_2 = _data.ticker
                if _ticker_2 > _ticker_1:
                    d_longer[_data.product][_data.date] = _data
    l_all_data = []
    for _product in d_longer.keys():
        for _date, _data in d_longer[_product].items():
            l_all_data.append(_data)
    return l_all_data


def run(name, new_data):
    path_file = os.path.join(PATH_OUTPUT, name + ".csv")
    path_file_bak = os.path.join(PATH_OUTPUT, name + "_" + datetime.now().strftime("%Y%m%d%H%M%S") + ".csv")
    
    # [1] 读取原 MostActivateTickerFile 文件的数据
    if os.path.isfile(path_file):
        old_data: List[MostActivateTickerFileData] = MostActivateTickerFile.read(path_file)
    else:
        old_data = []

    # [2] 检查数据, 新、旧数据是否有有冲突
    _error = False
    if old_data:
        for _data in new_data:
            old_ticker = MostActivateTickerFile.query_ticker_from_data(old_data, _data.date, _data.product)
            if not old_ticker:
                continue
            if _data.ticker != old_ticker:
                logger.error(f'db和文件数据不一致,{_data.date},{_data.product},{path_file}')
                _error = True
    if _error:
        run_warning_board(warning_msg='数据不一致')
        os.system('pause')
        raise Exception

    # [3] 合并数据
    all_data = old_data + new_data
    # 生成新的结果
    all_changed_data: List[MostActivateTickerFileData] = MostActivateTickerFile.gen_changed(all_data)

    # [4] 输出
    MostActivateTickerFile.write(p=path_file, data=all_changed_data)
    shutil.copyfile(path_file, path_file_bak)
    if IS_OUTPUT_TODAY:
        path_file_today_data = os.path.join(PATH_OUTPUT, f"_{name}_Today.csv")
        today_data = [i for i in all_changed_data if i.date == datetime.today().strftime('%Y%m%d')]
        MostActivateTickerFile.write(p=path_file_today_data, data=today_data)
        if today_data:
            pprint(today_data, indent=4)


if __name__ == '__main__':
    #
    logger = MyLogger('GenMostActivateTicker', output_root=os.path.join(PATH_ROOT, 'logs'))

    # [1] 从数据库下载数据
    d_config = json.loads(open(PATH_CONFIG).read())
    start_date = (datetime.now() + timedelta(days=START_DAY_OFFSET)).date()
    obj = MostActivateTickerToDB(
        **d_config,
        logger=logger
    )
    l_all_db_data: List[MostActivateTicker] = obj.download_from_db(start_date=start_date)

    # [2] 整理db数据 -> List[MostActivateTickerFileData]
    d_all_db_data = defaultdict(list)
    for _data in l_all_db_data:
        d_all_db_data[_data.Num].append(MostActivateTickerFileData.from_db_data(_data))

    # [3]
    l_infos = [
        {
            "name": "MostActivateTickers_1",
            "new_data": d_all_db_data.get(1)
        },
        {
            "name": "MostActivateTickers_2",
            "new_data": d_all_db_data.get(2)
        },
        {
            "name": "MostActivateTickers_2Longer",
            "new_data": gen_longer(d_all_db_data.get(1), d_all_db_data.get(2))
        },
    ]

    for info in l_infos:
        if not info["new_data"]:
            continue
        logger.info(f"handling {info['name']}")
        run(name=info["name"], new_data=info["new_data"])
