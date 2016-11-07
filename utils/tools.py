# coding=utf-8
import json
import logging
import time


def timeFormat(format, intTime):
    returnTime = time.strftime(format, time.localtime(intTime))
    return returnTime


def json2dict(s, log_path, app_name, timeYmd):
    try:
        dit = json.loads(s[1], encoding='utf-8')
        return dit
    except Exception as e:
        logout(log_path, app_name, timeYmd, 'Error Load Json: ' + s[1] + ' ' + str(e), 3)
        return {}


def logout(logRootPath, logName, timeYMD, message, level):
    # 创建一个logger
    logger = logging.getLogger(logName)
    logger.setLevel(logging.DEBUG)
    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler(logRootPath + logName + '-' + timeYMD)
    fh.setLevel(logging.DEBUG)
    # 再创建一个handler，用于输出到控制台
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.DEBUG)
    # 定义handler的输出格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    # ch.setFormatter(formatter)
    # 给logger添加handler
    logger.addHandler(fh)
    # logger.addHandler(ch)
    # 记录一条日志
    if level is 1:
        logger.info(message)
    elif level is 2:
        logger.warning(message)
    elif level is 3:
        logger.error(message)
    return 1

