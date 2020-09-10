from sqlalchemy import create_engine
from datetime import datetime
from ast import literal_eval
from cook import Archiver
import streamlit as st
import json
import boto3
import os

conn = create_engine(os.environ.get("RECIPE_ENGINE"))
archiver = Archiver(
    engine=os.environ["RECIPE_ENGINE"],
    ftp_prefix=os.environ["FTP_PREFIX"],
    s3_endpoint=os.environ.get("AWS_S3_ENDPOINT", "").replace("https://", ""),
    s3_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    s3_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", ""),
)
session = boto3.session.Session()
client = session.client(
    "s3",
    region_name="nyc3",
    endpoint_url=os.environ.get("AWS_S3_ENDPOINT", ""),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", ""),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
)


def get_schema():
    schemas = conn.execute("select distinct schema_name from meta.metadata").fetchall()
    return [dict(row)["schema_name"] for row in schemas]


def get_metadata(schema):
    result = conn.execute(
        f"select * from meta.metadata where schema_name = '{schema}'"
    ).fetchone()
    if result is None:
        return None, {}, None
    else:
        return result


def get_tables(schema):
    tables = conn.execute(
        f"select * from information_schema.tables where table_schema = '{schema}'"
    ).fetchall()
    return [dict(row)["table_name"] for row in tables]


def get_latest(schema):
    tables = conn.execute(
        f"""
        SELECT table_name
        FROM information_schema.view_table_usage
        WHERE view_schema = '{schema}' and view_name= 'latest'
        """
    ).fetchall()
    return [dict(row)["table_name"] for row in tables][0]


def write_to_s3(newfile, schema, version_name, acl, ext, client=client):
    if newfile is not None:
        key = f'{datetime.today().strftime("%Y-%m-%d")}/{schema}-{version_name}.{ext}'
        bucket = "edm-recipes"
        client.put_object(ACL=acl, Body=newfile.getvalue(), Bucket=bucket, Key=key)
        path = f"s3://{bucket}/{key}"
        st.success(f"successfully uploaded to {path}")
        return path
    else:
        st.warning("awaiting your file ...")
        return ""


# MAIN
st.markdown(
    """
    <h1 style="font-size:3rem;">🍔 Data Recipes</h1>
    """,
    unsafe_allow_html=True,
)
st.sidebar.markdown(
    """
    <div stule="margin-left: auto; margin-right: auto;">
    <img style='width:40%; margin: 0 auto 2rem auto;display:block;'
        src="https://raw.githubusercontent.com/NYCPlanning/logo/master/dcp_logo_772.png">
    </div>
    """,
    unsafe_allow_html=True,
)

schemas = get_schema()
new = st.checkbox("new table?")
if new:
    schema = st.text_input("pick a table name")
else:
    schema = st.selectbox(
        "pick a table name", schemas, index=schemas.index("dpr_parksproperties")
    )

tables = get_tables(schema)
_, metadata, last_update = get_metadata(schema)
st.text(last_update)
st.write(tables)

version_name = st.text_input("version_name", metadata.get("version_name", ""))
dstSRS = st.text_input("dstSRS", metadata.get("dstSRS", "EPSG:4326"))
srcSRS = st.text_input("srcSRS", metadata.get("srcSRS", "EPSG:4326"))
geometryType = st.text_input("geometryType", metadata.get("geometryType", "NONE"))
layerCreationOptions = st.text_input(
    "layerCreationOptions",
    metadata.get("layerCreationOptions", "['OVERWRITE=YES', 'PRECISION=NO']"),
)
newFieldNames = st.text_input("newFieldNames", metadata.get("newFieldNames", "[]"))
srcOpenOptions = st.text_input(
    "srcOpenOptions",
    metadata.get(
        "srcOpenOptions",
        "['AUTODETECT_TYPE=NO', 'EMPTY_STRING_AS_NULL=YES', 'GEOM_POSSIBLE_NAMES=the_geom']",
    ),
)
metaInfo = st.text_area("metaInfo", metadata.get("metaInfo", ""))
upload = st.checkbox("upload new file?")
edit = st.checkbox("edit metadata w/o upload?")

_path = metadata.get("path", "")
if upload:
    acl = st.radio("ACL", ("public-read", "private"), index=1)
    ext = st.selectbox("Pick your file type", ["csv", "geojson", "zip"], index=0)
    newfile = st.file_uploader("upload new", type=[ext])
    path = write_to_s3(newfile, schema, version_name, acl, ext, client=client)
else:
    path = st.text_input("path", _path)

recipe_config = {
    "path": path,
    "dstSRS": dstSRS,
    "srcSRS": srcSRS,
    "metaInfo": metaInfo,
    "schema_name": schema,
    "geometryType": geometryType,
    "version_name": version_name,
    "newFieldNames": literal_eval(newFieldNames),
    "srcOpenOptions": literal_eval(srcOpenOptions),
    "layerCreationOptions": literal_eval(layerCreationOptions),
}
submit = st.button("submit")

# SIDEBAR
st.sidebar.markdown(
    """
    # Instructions:
    ### Updating an Exisiting Table:
    1. select the __schema name__ (table name) you are looking for, note the dropdown supports text search.
    2. enter a __version name__. For pluto, DCP published shapfiles, use vintage, e.g. `20A`, `20B`, `20v2` and etc
    3. Specify __spatial reference__, e.g. `EPSG:4326`, `EPSG:2263`
    4. Specify __geometry type__, one of `NONE`, `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `GEOMETRYCOLLECTION`,
    `MULTIPOINT`, `MULTIPOLYGON`, `MULTILINESTRING`, `CIRCULARSTRING`, `COMPOUNDCURVE`, `CURVEPOLYGON`, `MULTICURVE`, and `MULTISURFACE`
    5. Specify __layer creation options__, e.g. `['OVERWRITE=YES', 'PRECISION=NO']`
    6. __SRC open options__, e.g. `['AUTODETECT_TYPE=NO', 'EMPTY_STRING_AS_NULL=YES', 'GEOM_POSSIBLE_NAMES=the_geom', 'X_POSSIBLE_NAMES=longitude,Longitude,Lon,lon,x', 'Y_POSSIBLE_NAMES=latitude,Latitude,Lat,lat,y']`
    7. Only fill in __new field names__ if we have a new list of columns names, make sure the order matches
    8. __Metainfo__ is for a brief description about the dataset, e.g. "data comes from opendata"
    9. Choose if you are loading data from a known url or upload new file.

    ###  Create New Table:
    1. choose a schema name that hasn't been used before.
    2. fill in the corresponding values according the above mentioned.
    """
)

st.sidebar.header("Delete Table")
if not new:
    latest = get_latest(schema)
    undeletable = [latest, "latest"]
    deletable = [i for i in tables if i not in undeletable]
    del_table = st.sidebar.multiselect(
        "pick a version to delete", deletable, key="del-select-table"
    )
    delete = st.sidebar.button("delete")
    st.sidebar.warning(
        f'note that {", ".join([f"`{i}`" for i in undeletable])} \
        are not deletable, if you want to over write, just upload a new version'
    )
    if delete:
        for i in del_table:
            conn.execute(
                f"""
                DROP TABLE {schema}."{i}";
            """
            )
            st.sidebar.success(f"`{schema}.{i}` is deleted")
else:
    pass
st.sidebar.markdown(
    """
    This application is still in active development.
    It is created by and maintained by __EDM Data Engineering__
    [issues](https://github.com/NYCPlanning/recipe-app/issues) are welcomed!
""",
    unsafe_allow_html=True,
)
###

if submit and not new:
    if not edit:
        archiver.archive_table(recipe_config)
        conn.execute(
            f"""
                UPDATE meta.metadata
                SET config = %(config)s ::jsonb,
                    last_update = now()
                WHERE schema_name=%(schema_name)s;
            """,
            {"config": json.dumps(recipe_config), "schema_name": schema,},
        )
    else:
        conn.execute(
            f"""
                UPDATE meta.metadata
                SET config = %(config)s ::jsonb
                WHERE schema_name=%(schema_name)s;
            """,
            {"config": json.dumps(recipe_config), "schema_name": schema,},
        )
    st.success(f"{schema} is successfully loaded")
    st.write(recipe_config)

elif submit and new:
    if not edit:
        archiver.archive_table(recipe_config)

    conn.execute(
        f"""
            INSERT INTO meta.metadata(schema_name, config,last_update)
            VALUES
                (%(schema_name)s, %(config)s ::jsonb, now());
        """,
        {"config": json.dumps(recipe_config), "schema_name": schema,},
    )
    st.success(f"{schema} is successfully loaded")
    st.write(recipe_config)
else:
    pass
