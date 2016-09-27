#!/usr/bin/env python
# encoding: utf-8
# Author:  Xzp
# Date: 2016/8/30 0030 上午 11:21

data={"App":"sxeqkkyoocom","AudioBitRate":0,"AudioCodecId":10,"AudioSampleRate":0,"EmptyNumbers":8,"EmptyNumbersIn60s":1,"EmptyTime":3753,"EmptyTimeIn60s":404,"EndTime":1472092518,"FirstBufferTime":83,"Max60sFluency":100,"MaxEmptyTime":759,"Min60sFluency":97.84,"PublishTime":1437955,"RecvByteSum":24424603,"RemoteIp":"139.212.13.83","SendByteSum":29305,"StartTime":1472091080,"Stream":"11199900","VideoBitRate":0,"VideoCodecId":7,"VideoFrameRate":0,"VideoHeigth":0,"VideoKeyFrameInterval":0,"VideoWidth":0,"SvrIp":"218.244.149.108","PlayTime":1434119,"PlayTimeIn60s":59513,"PlayFluency":99.73,"PlayFluencyIn60s":99.19,"Cmd":"publish","User":"1015"}
app = data['App']
stream = data['Stream']
user = data['User']
cmd = data['Cmd']
timestamp = data['EndTime']
print(app, stream,user,cmd,timestamp)

