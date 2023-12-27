bigint = 'bigint'
'''(int8)	signed eight-byte integer'''
bigserial = 'bigserial'
'''(serial8)	autoincrementing eight-byte integer'''
def bit(n:int = 1): 
    '''fixed-length bit string'''
    return f'bit({n})'
def varbit(n:int = None): 
    '''variable-length bit string''' 
    return f'varbit({n})' if n else 'varbit'

boolean = 'boolean'
'''(bool) logical Boolean (true/false)'''

box = 'box'
'''rectangular box on a plane'''
bytea = 'bytea'
'''binary data ("byte array")'''
def character(n:int = 1):
    '''fixed-length character string'''
    return f'character({n})'
def varchar(n:int = None):
    '''
    variable-length character string.
    Parameter
    -
    n (int): default None is none limit.
    '''
    return f'varchar({n})' if n else 'varchar'

cidr = 'cidr'
'''IPv4 or IPv6 network address'''
circle = 'circle'
'''circle on a plane'''
date = 'date'
'''calendar date (year, month, day)'''
double = 'double precision'
'''(float8)	double precision floating-point number (8 bytes)'''
inet = 'inet'
'''IPv4 or IPv6 host address'''
integer = 'integer'
'''(int), (int4) signed four-byte integer'''
def interval_f(precision:str):
    '''time span front'''
    # select now(), now() - interval'30 minute';
    return f'interval\'{precision}\''
def interval_b(precision:str):
    '''time span back'''
    # select now(), now()::date - '1 day'::interval;
    return f'\'{precision}\'::interval'

json = 'json'
'''textual JSON data'''
jsonb = 'jsonb'
'''binary JSON data, decomposed'''
line = 'line'
'''infinite line on a plane'''
lseg = 'lseg'
'''line segment on a plane'''
macaddr = 'macaddr'
'''MAC (Media Access Control) address'''
macaddr8 = 'macaddr8'
'''MAC (Media Access Control) address (EUI-64 format)'''
money = 'money'
'''currency amount'''
def numeric(precision, scale):
    '''exact numeric of selectable precision'''
    return f'numeric({precision}, {scale})'
def decimal(precision, scale):
    '''exact numeric of selectable precision'''
    return f'decimal({precision}, {scale})'

path = 'path'
'''geometric path on a plane'''
pg_lsn = 'pg_lsn'
'''PostgreSQL Log Sequence Number'''
pg_snapshot = 'pg_snapshot'
'''user-level transaction ID snapshot'''
point = 'point'
'''geometric point on a plane'''
polygon = 'polygon'
'''closed geometric path on a plane'''
real = 'real'
'''(float4)	single precision floating-point number (4 bytes)'''
smallint = 'smallint'
'''(int2)	signed two-byte integer'''
smallserial = 'smallserial'
'''(serial2)	autoincrementing two-byte integer'''
serial = 'serial'
'''(serial4)	autoincrementing four-byte integer'''
text = 'text'
'''variable-length character string'''
def time(precision, without_time_zone:bool = True):
    '''time of day\n
    timetz including time zone'''
    return f'time({precision})' if without_time_zone else f'time({precision}) with time zone'
def timestamp(precision, without_time_zone:bool = True):
    '''date and time\n
    timestamptz	date and time, including time zone'''
    return f'timestamp({precision})' if without_time_zone else f'timestamp({precision}) with time zone'

tsquery = 'tsquery'
'''text search query'''
tsvector = 'tsvector'
'''text search document'''

uuid = 'uuid'
'''universally unique identifier'''
xml = 'xml'
'''XML data'''