import streamlit as st
from sqlalchemy import create_engine
import os
from ast import literal_eval
from cook import Archiver
import json

conn = create_engine(os.environ.get('RECIPE_ENGINE'))
archiver = Archiver(
        engine=os.environ['RECIPE_ENGINE'], 
        ftp_prefix=os.environ['FTP_PREFIX'],
        s3_endpoint = os.environ.get('AWS_S3_ENDPOINT', ''),
        s3_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
        s3_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID', '')
    )

st.header('recipe')

@st.cache
def get_schema():
    schemas = conn.execute('select distinct table_schema from information_schema.tables where table_schema !~*\'meta\'').fetchall()
    return [dict(row)['table_schema'] for row in schemas]

@st.cache
def get_metadata(schema):
    return conn.execute(f"select * from meta.metadata where schema_name = '{schema}'").fetchone()

@st.cache
def get_tables(schema): 
    tables = conn.execute(f"select * from information_schema.tables where table_schema = '{schema}'").fetchall()
    return [dict(row)['table_name'] for row in tables]

schemas = get_schema()
schema = st.selectbox('pick a table name', schemas, index=schemas.index('test'))
tables = get_tables(schema)
_, metadata, last_update = get_metadata(schema)
st.text(last_update)
st.write(tables)

version_name = st.text_input('version_name', metadata['version_name'])
dstSRS = st.text_input('dstSRS', metadata['dstSRS'])
srcSRS = st.text_input('srcSRS', metadata['srcSRS'])
metaInfo = st.text_input('metaInfo', metadata.get('metaInfo', ''))
geometryType = st.text_input('geometryType', metadata['geometryType'])
newFieldNames = st.text_input('newFieldNames', metadata['newFieldNames'])
srcOpenOptions = st.text_input('srcOpenOptions', metadata['srcOpenOptions'])
layerCreationOptions = st.text_input('layerCreationOptions', metadata['layerCreationOptions'])
path = st.text_input('path', metadata['path'])

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

if submit:
    archiver.archive_table(recipe_config)
    conn.execute(f'''
            UPDATE meta.metadata
            SET config='{json.dumps(recipe_config)}'::jsonb,
                last_update=now()
            WHERE schema_name='{schema}';
        ''')
    st.success(f'{schema} is successfully loaded')
    st.write(recipe_config)
else:
    pass