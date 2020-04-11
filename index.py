from sqlalchemy import create_engine
from datetime import datetime
from ast import literal_eval
from cook import Archiver
import streamlit as st
import json
import magic
import boto3
import os

conn = create_engine(
            os.environ.get('RECIPE_ENGINE'))
archiver = Archiver(
            engine=os.environ['RECIPE_ENGINE'], 
            ftp_prefix=os.environ['FTP_PREFIX'],
            s3_endpoint = os.environ.get('AWS_S3_ENDPOINT', '').replace('https://', ''),
            s3_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
            s3_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID', ''))
session = boto3.session.Session()
client = session.client('s3',
            region_name='S3_REGION',
            endpoint_url=os.environ.get('AWS_S3_ENDPOINT', ''),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', ''),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', ''))

st.header('recipe')

@st.cache
def get_schema():
    schemas = conn.execute('select distinct schema_name from meta.metadata').fetchall()
    return [dict(row)['schema_name'] for row in schemas]

@st.cache
def get_metadata(schema):
    return conn.execute(f"select * from meta.metadata where schema_name = '{schema}'").fetchone()

@st.cache
def get_tables(schema): 
    tables = conn.execute(f"select * from information_schema.tables where table_schema = '{schema}'").fetchall()
    return [dict(row)['table_name'] for row in tables]

def guess_type(newfile):
    with magic.Magic() as m:
        filetype=m.id_buffer(newfile.getvalue())
    if 'CSV text' in filetype:
        return 'csv'
    elif 'Zip archive' in filetype:
        return 'zip'
    else:
        return 'geojson'

def write_to_s3(newfile, schema, acl='client', client=client):
    if newfile is not None:
        extension=guess_type(newfile)
        key=f'{datetime.today().strftime("%Y-%m-%d")}/{schema}.{extension}'
        bucket='edm-recipes'
        client.put_object(
                ACL=acl, 
                Body=newfile.getvalue(), 
                Bucket=bucket,
                Key=key)
        path=f's3://{bucket}/{key}'
        st.success(f'successfully uploaded to {path}')
        return path
    else: 
        st.warning('awaiting your file ...')
        return ''

schemas = get_schema()
new = st.checkbox('new table?')
if new:
    schema = st.text_input('pick a table name')
    metadata = {}
else: 
    schema = st.selectbox('pick a table name', schemas, index=schemas.index('dpr_parksproperties'))
    tables = get_tables(schema)
    _, metadata, last_update = get_metadata(schema)
    st.text(last_update)
    st.write(tables)

version_name = st.text_input('version_name', metadata.get('version_name', ''))
dstSRS = st.text_input('dstSRS', metadata.get('dstSRS', 'EPSG:4326'))
srcSRS = st.text_input('srcSRS', metadata.get('dstSRS', 'EPSG:4326'))
metaInfo = st.text_input('metaInfo', metadata.get('metaInfo', ''))
geometryType = st.text_input('geometryType', metadata.get('geometryType', 'NONE'))
newFieldNames = st.text_input('newFieldNames', metadata.get('newFieldNames', '[]'))
srcOpenOptions = st.text_input('srcOpenOptions', metadata.get('srcOpenOptions', "['AUTODETECT_TYPE=NO', 'EMPTY_STRING_AS_NULL=YES', 'GEOM_POSSIBLE_NAMES=the_geom']"))
layerCreationOptions = st.text_input('layerCreationOptions', metadata.get('layerCreationOptions', "['OVERWRITE=YES', 'PRECISION=NO']"))
upload=st.checkbox('upload new file?')
if upload:
    acl = st.radio('ACL', ('public-read', 'private'))
    newfile = st.file_uploader('upload new', type=['csv', 'zip', 'geojson'])
    path = write_to_s3(newfile, schema, acl, client=client)
else: 
    path = st.text_input('path', metadata.get('path', ''))

recipe_config={
    "path":path,
    "dstSRS":dstSRS,
    "srcSRS":srcSRS,
    "metaInfo":metaInfo,
    "schema_name":schema,
    "geometryType":geometryType,
    "version_name":version_name,
    "newFieldNames":literal_eval(newFieldNames),
    "srcOpenOptions":literal_eval(srcOpenOptions),
    "layerCreationOptions":literal_eval(layerCreationOptions)
    }
submit = st.button('submit')

if submit and not new:
    archiver.archive_table(recipe_config)
    conn.execute(f'''
            UPDATE meta.metadata
            SET config='{json.dumps(recipe_config)}'::jsonb,
                last_update=now()
            WHERE schema_name='{schema}';
        ''')
    st.success(f'{schema} is successfully loaded')
    st.write(recipe_config)
elif submit and new: 
    archiver.archive_table(recipe_config)
    conn.execute(f'''
            INSERT INTO meta.metadata(schema_name, config,last_update)
            VALUES 
                ('{schema}',
                '{json.dumps(recipe_config)}'::jsonb,
                now());
        ''')
    st.success(f'{schema} is successfully loaded')
    st.write(recipe_config)
else:
    pass