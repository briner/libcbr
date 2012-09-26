'''
Created on Dec 6, 2011

@author: briner

http://en.wikipedia.org/wiki/ISO_8601


# D U R A T I O N
#
# duration is of the form: PnYnMnDTnHnMnS
#                      or: PnW
#                      or: PYYYYMMDDThhmmss
#                      or: P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]
# where:
#    P is the duration designator (historically called "period") placed at the start of the duration representation.
#    Y is the year designator that follows the value for the number of years.
#    M is the month designator that follows the value for the number of months.
#    W is the week designator that follows the value for the number of weeks.
#    D is the day designator that follows the value for the number of days.
#    T is the time designator that precedes the time components of the representation.
#    H is the hour designator that follows the value for the number of hours.
#    M is the minute designator that follows the value for the number of minutes.
#    S is the second designator that follows the value for the number of seconds.
'''

import datetime
import re
from exceptions import ValueError
import logging

from dateutil.relativedelta import relativedelta


my_logger = logging.getLogger('MyLogger')

dregister_duration={}
def register_duration(name, fun):
    global dregister_duration
    dregister_duration[name]=fun

def strdict2intdict(str_dict):
    int_dict={}
    for k,v in str_dict.iteritems():
        if v:
            int_dict[k]=int(v)
        else:
            int_dict[k]=0
    return int_dict

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
#       D U R A T I O N
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# 
# for PnYnMnDTnHnMnS
RE_STR_DURATION_DATE='(?:(?P<year>\d+)Y)?(?:(?P<month>\d+)M)?(?:(?P<day>\d+)D)?'
RE_STR_DURATION_TIME='(?:(?P<hour>\d+)H)?(?:(?P<minute>\d+)M)?(?:(?P<second>\d+)S)?'
RE_STR_DURATION_1='^P(%s)?(T%s)?$' % (RE_STR_DURATION_DATE, RE_STR_DURATION_TIME )
RE_DURATION_PnYnMnDTnHnMnS=re.compile(RE_STR_DURATION_1)
def parse_duration_PnYnMnDTnHnMnS(str_iso):
    '''PnYnMnDTnHnMnS'''
    match=RE_DURATION_PnYnMnDTnHnMnS.search(str_iso)
    if match:
        str_dict=match.groupdict()
        int_dict=strdict2intdict(str_dict)
        rd=relativedelta(years=int_dict['year'],
                         months=int_dict['month'],
                         days=int_dict['day'],
                         hours=int_dict['hour'],
                         minutes=int_dict['minute'],
                         seconds=int_dict['second'])
        return rd
    else:
        return None
register_duration('PnYnMnDTnHnMnS', parse_duration_PnYnMnDTnHnMnS)

#
# for PnW
RE_DURATION_WEEK=re.compile('^P(?P<week>\d+)W$')
def parse_duration_week(str_iso):
    '''PnW'''
    match=RE_DURATION_WEEK.search(str_iso)
    if match:
        str_dict=match.groupdict()
        int_dict=strdict2intdict(str_dict)
        rd=relativedelta(weeks=int_dict['week'])
        return rd
    else:
        return None   
register_duration('PnW', parse_duration_week)

#
# for PYYYYMMDDThhmmss 
RE_DURATION_PYYYYMMDDThhmmss=re.compile('^P(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})T(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})$')
def parse_duration_PYYYYMMDDThhmmss(str_iso):
    '''PYYYYMMDDThhmmss'''
    match=RE_DURATION_PYYYYMMDDThhmmss.search(str_iso)
    if not match:
        return None
    str_dict=match.groupdict()
    int_dict=strdict2intdict(str_dict)
    #
    # test
    if int_dict['month']>12:
        raise ValueError('month can not be higher than 12')
    if int_dict['day']>31:
        raise ValueError('day can not be higher than 31')
    if int_dict['hour']>23:
        raise ValueError('hour can not be higher than 23')
    if int_dict['minute']>59:
        raise ValueError('minute can not be higher than 59')
    if int_dict['second']>59:
        raise ValueError('seconde can not be higher than 59')
    #
    rd=relativedelta(years=int_dict['year'],
                     months=int_dict['month'],
                     days=int_dict['day'],
                     hours=int_dict['hour'],
                     minutes=int_dict['minute'],
                     seconds=int_dict['second'])
    return rd
register_duration('PYYYYMMDDThhmmss', parse_duration_PYYYYMMDDThhmmss)

#
# for P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]
RE_DURATION_PYYYY_MM_DDThh_mm_ss=re.compile('^P(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})$')
def parse_duration_PYYYY_MM_DDThh_mm_ss(str_iso):
    '''P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]'''
    match=RE_DURATION_PYYYY_MM_DDThh_mm_ss.search(str_iso)
    if not match:
        return None
    str_dict=match.groupdict()
    int_dict=strdict2intdict(str_dict)
    #
    # test
    if int_dict['month']>12:
        raise ValueError('month can not be higher than 12')
    if int_dict['day']>31:
        raise ValueError('day can not be higher than 31')
    if int_dict['hour']>23:
        raise ValueError('hour can not be higher than 23')
    if int_dict['minute']>59:
        raise ValueError('minute can not be higher than 59')
    if int_dict['second']>59:
        raise ValueError('second can not be higher than 59')
    #
    rd=relativedelta(years=int_dict['year'],
                     months=int_dict['month'],
                     days=int_dict['day'],
                     hours=int_dict['hour'],
                     minutes=int_dict['minute'],
                     seconds=int_dict['second'])
    return rd
register_duration('P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]', parse_duration_PYYYY_MM_DDThh_mm_ss)
    

#
# _parse
def parse_duration(str_iso):
    if str_iso:
        if str_iso[0]!='P':
            raise Iso8601Error()
    for name, fun in dregister_duration.iteritems():
        int_dict=fun(str_iso)
        if int_dict:
            my_logger.debug('str_iso(%s) decoded with format(%s) of type(duration)' % (str_iso, name) )
            return int_dict
    raise Iso8601FormatError()

 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
#       C A L E N D A R   D A T E
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# YYYY-MM-DD or YYYY-MM or YYYYMMDD
RE_STR_CALENDAR_DATE='(?P<year>\d{4})(?P<sep>-)?(?P<month>\d{2})(?#aie)(?(sep)(-(?P<day1>\d{2}))?|(?P<day2>\d{2}))'
RE_CALENDAR_DATE=re.compile(RE_STR_CALENDAR_DATE)
def parse_calendar_date(str_iso):
    match=RE_CALENDAR_DATE.match(str_iso)
    if match:
        my_logger.debug('str_iso(%s) decoded of type(calendar_date)' % str_iso )
        dict_iso=match.groupdict()
        day=0
        is_only_year_n_month=True
        if dict_iso['day1']:
            is_only_year_n_month=False
            day+=int(dict_iso['day1'])
        if dict_iso['day2']:
            is_only_year_n_month=False
            day+=int(dict_iso['day2'])
        if is_only_year_n_month:
            day=1
        month=int(dict_iso['month'])
        year=int(dict_iso['year'])
        return datetime.datetime(year, month, day)
    return None

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
#       
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class Iso8601Error(Exception): pass
class Iso8601FormatError(Iso8601Error):
    '''used when the format is not followed (eg.)'''
    pass

def parse(str_iso):
    duration=None
    try:
        duration=parse_duration(str_iso)
    except Iso8601FormatError, inst_error:
        raise inst_error
    except Iso8601Error:
        pass
    if duration:
        return duration
    #
    calendar_date=parse_calendar_date(str_iso)
    if calendar_date:
        return calendar_date
        return calendar_date
    raise Iso8601Error('neither a Duration object nor a CalendarDate was generated')

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
#       T E S T
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
import unittest
class TestSequenceFunctions(unittest.TestCase):
    def test_PnYnMnDTnHnMnS_1(self):
        duration=parse('P1975Y8M19DT22H10M34S')
        duration1=parse('P1Y2M3D')
        datetime1=datetime.datetime(1975,  8, 19, 12, 15, 20)
        datetime_duration1=datetime1+duration1
        datetime2=datetime.datetime(1976, 10, 22, 12, 15, 20)
        self.assertEqual(datetime_duration1, datetime2)
    def test_PnYnMnDTnHnMnS_2(self):
        duration1=parse('P1Y2M3D')
        datetime1=datetime.datetime(1975,  8, 19, 12, 15, 20)
        datetime_duration1=datetime1+duration1
        datetime2=datetime.datetime(1976, 10, 22, 12, 15, 20)
        self.assertEqual(datetime_duration1, datetime2)
    def test_PnYnMnDTnHnMnS_3(self):
        duration1=parse('PT4H5M6S')
        datetime1=datetime.datetime(1975, 8, 19, 12, 15, 20)
        datetime_duration1=datetime1+duration1
        datetime2=datetime.datetime(1975, 8, 19, 16, 20, 26)
    def test_PnYnMnDTnHnMnS_4(self):
        duration1=parse('PT10M')
        datetime1=datetime.datetime(1975, 8, 19, 12, 15, 20)
        datetime_duration1=datetime1+duration1
        datetime2=datetime.datetime(1975, 8, 19, 12, 25, 20)
        self.assertEqual(datetime_duration1, datetime2)
    def test_PnYnMnDTnHnMnS_5(self):
        self.assertRaises(Iso8601FormatError, parse, 'P8M1975Y' )
    def test_PnW_1(self):
        duration1=parse('P3W')
        datetime1=datetime.datetime(1975, 8, 19, 12, 15, 20)
        datetime_duration1=datetime1+duration1
        datetime2=datetime.datetime(1975, 9, 9, 12, 15, 20)
        self.assertEqual(datetime_duration1, datetime2)
    def test_PYYYYMMDDThhmmss_1(self):
        duration1=parse('P00010203T040506')
        datetime1=datetime.datetime(1975, 8, 19, 12, 15, 20)
        datetime_duration1=datetime1+duration1
        datetime2=datetime.datetime(1976, 10, 22, 16, 20, 26)
        self.assertEqual(datetime_duration1, datetime2)
    def test_PYYYYMMDDThhmmss_2(self):
        self.assertRaises(ValueError, parse, 'P19751319T112233')
    #
    # P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]
    def test_duration_PYYYY_MM_DDThh_mm_ss_1(self):
        duration1=parse('P0001-02-03T04:05:06')
        datetime1=datetime.datetime(1975, 8, 19, 12, 15, 20)
        datetime_duration1=datetime1+duration1
        datetime2=datetime.datetime(1976, 10, 22, 16, 20, 26)
        self.assertEqual(datetime_duration1, datetime2)
    def test_duration_PYYYY_MM_DDThh_mm_ss_2(self):
        self.assertRaises(Iso8601FormatError, parse, 'P1975-08-19T11:22')
    def test_duration_PYYYY_MM_DDThh_mm_ss_3(self):
         self.assertRaises(ValueError, parse, 'P1975-08-19T11:70:33')
    #
    # calendar_date YYYY-MM-DD or YYYY-MM or YYYYMMDD
    # 
    def test_calendar_date_1(self):
        calendar_date=parse('1975-08-19')
        self.assertIsInstance(calendar_date, datetime.date)
        self.assertEqual(calendar_date.year, 1975)
        self.assertEqual(calendar_date.month, 8)
        self.assertEqual(calendar_date.day, 19)
    def test_calendar_date_2(self):
        calendar_date=parse('1975-08')
        self.assertIsInstance(calendar_date, datetime.date)
        self.assertEqual(calendar_date.year, 1975)
        self.assertEqual(calendar_date.month, 8)
        self.assertEqual(calendar_date.day, 1)
    def test_calendar_date_3(self):
        calendar_date=parse('19750819')
        self.assertIsInstance(calendar_date, datetime.date)
        self.assertEqual(calendar_date.year, 1975)
        self.assertEqual(calendar_date.month, 8)
        self.assertEqual(calendar_date.day, 19)
    def test_calendar_date_4(self):
        self.assertRaises(ValueError, parse, '19750832')

