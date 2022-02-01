import os
import requests
import bs4
import typing
from remotezip import RemoteZip
from osgeo import gdal, ogr, gdalconst
import logging
logger = logging.getLogger(__name__)
import math
gdal.UseExceptions()
import  rioxarray
import numpy as np
import rasterio
import multiprocessing as mp
from ndpyramid.utils import multiscales_template, get_version
import xarray as xr
import json
import datatree as dt
import time
import getpass
import sys
import socket
from pipeline import __version__
from urllib.parse import urlparse
from carbonplan_data.utils import set_zarr_encoding


def get_cf_global_attrs(**attrs):

    if "history" not in attrs:
        attrs["history"] = "Created: {}".format(time.ctime(time.time()))

    if "insitution" not in attrs:
        attrs["institution"] = "CarbonPlan"

    if "source" not in attrs:
        attrs["source"] = sys.argv[0]

    if "hostname" not in attrs:
        attrs["hostname"] = socket.gethostname()

    if "username" not in attrs:
        attrs["username"] = getpass.getuser()

    if "version" not in attrs:
        attrs["version"] = __version__

    return attrs



def zooml2resmeters(zoom: int = 0, tile_size=256):
    initial_res = 2 * math.pi * 6378137 / tile_size
    return initial_res / (2**zoom)


def res2zoom(resm=None, tile_size=256):
    initial_res = 2 * math.pi * 6378137 / tile_size

    return math.log(abs(resm)/initial_res, 2)


def fetch_hrea_links(url: str='http://www-personal.umich.edu/~brianmin/HREA/data.html')-> typing.Dict :
    """
    Fethches dropbox download links for High Resolution Electricity Access database (HREA)
    Credit goes  to Brian Min and Zachary O'Keeffe from University of Michigan fro developing
    HREA with support from World Bank


    :param url, str,:
    :return: a 2 level dictionary country/year as per hos the data is organized

    """
    # stgore locally, no reason to spam
    n = './data'
    cond = os.path.exists(n) and os.path.getsize(n) > 0
    if not cond:
        with open(n, 'w') as d:
            with requests.get(url=url) as r:
                content = r.content
                d.write(content.decode('utf-8'))

    else:
        with open(n) as dc:
            content = dc.read()


    soup = bs4.BeautifulSoup(content, 'html5lib')


    countries = soup.find('ul', { 'id' : 'countrynames' })
    rd = {}
    for e in countries.find_all_next('h5'):

        country = e.get_text().strip()

        if len(e.find_next_siblings('ul')) == 1:
            rd[country] = {}
            for ue in e.find_next_siblings('ul'):
                for ee in ue.children:
                    try:
                        year, a =  ee.contents
                        link = a.get('href')
                        # to be able to download from Dropbox
                        link = link.replace('=0', '=1')
                        rd[country][int(year.strip())] = link

                    except AttributeError:
                        pass
                    except ValueError as ve:
                        pass
            #print(country, len(rd[country]))

    return rd


def dataset_from_url(src_zip: str = None, varname: str = 'lightscore', tmp_folder='/tmp') -> str:
    """
    Compose a GDAL vsizip url for the  HREA geotif file located inside the remote located
    src_zip whose  file name contains string "varname"
    varname
    :param src_zip:
    :param varname:
    :return:
    """


    with RemoteZip(src_zip) as szip:
        for zip_info in szip.infolist():

            if varname in zip_info.filename:
                vsipath = f'/vsimem/{zip_info.filename}'
                if tmp_folder:
                    dst_path = os.path.join(tmp_folder, zip_info.filename)
                    if os.path.exists(dst_path):
                        logger.debug(f'Reusing {dst_path} for {src_zip}/{zip_info.filename}')
                        return dst_path

                #return  f'zip:///{src_zip}!/{zip_info.filename}'
                #return  f'{src_zip}/{zip_info.filename}'
                #return  f'/vsizip/vsicurl/{src_zip}/{zip_info.filename}'

                with szip.open(zip_info.filename, 'r') as r:
                    logger.debug(f'Downloading {zip_info.filename} from {src_zip} into {vsipath}')
                    e = gdal.FileFromMemBuffer(vsipath, r.read())

                    #ds = gdal.OpenEx(vsipath)
                    if tmp_folder:
                        dst_path = os.path.join(tmp_folder, zip_info.filename)
                        ds = gdal.Translate(dst_path, vsipath)
                        ds = None
                    else:
                         dst_path = vsipath
                return dst_path


from azure.storage.blob.aio import ContainerClient

async def list_az_cogs(
        hrea_sas_url: str =None,
        country: str=None,
        year: int=None,
        var_name: str=None,
        for_gdal=False,
        for_cogeo=False
        ) -> typing.List[str]:

    async  with ContainerClient.from_container_url(hrea_sas_url) as client:

        blob_url_list = list()
        async for blob in client.list_blobs():
            bname = blob.name

            blob_client = client.get_blob_client(bname)

            if country and not country in bname: continue
            if year and not str(year) in bname:continue
            if var_name and not var_name in bname:continue
            if for_gdal:
                bname = f'/vsiaz_streaming/{client.container_name}/{bname}'
            if for_cogeo:
                bname = blob_client.url
            blob_url_list.append(bname)

        return blob_url_list







def pyramid_coarsen(ds, factors: typing.List[int], dims: typing.List[str], **kwargs) -> dt.DataTree:

    # multiscales spec
    save_kwargs = locals()
    del save_kwargs['ds']

    attrs = {
        'multiscales': multiscales_template(
            datasets=[{'path': str(i)} for i in range(len(factors))],
            type='reduce',
            method='pyramid_coarsen',
            version=get_version(),
            kwargs=save_kwargs,
        )
    }

    # set up pyramid
    root = xr.Dataset(attrs=attrs)
    pyramid = dt.DataTree(name='root', data=root)

    # pyramid data
    for key, factor in enumerate(factors):

        skey = str(key)
        kwargs.update({d: factor for d in dims})
        pyramid[skey] = ds.coarsen(**kwargs).mean()

    return pyramid


import dask
import xarray
from rasterio.vrt import WarpedVRT
import numcodecs


def cog2zarr(src_path: str = None, var_name: str = None, dst_proj='EPSG:3857', levels=list(range(15)), ):
    kwargs = locals()
    attrs1 = {
        'multiscales': multiscales_template(
            datasets=[{'path': str(i)} for i in levels],
            type='reduce',
            method='cog2zarr',
            version=get_version(),
            kwargs=kwargs,
        )
    }
    attrs = get_cf_global_attrs()
    attrs.update({'institution': 'UNDP Geo analytics'})
    attrs1.update(attrs)


    root = xr.Dataset(attrs=attrs1)
    print(json.dumps(root.attrs, indent=4))
    pyramid = dt.DataTree(name='root', data=root)

    codec_config = {"id": "zlib", "level": 1}
    compressor = numcodecs.get_codec(codec_config)

    with rasterio.open(src_path) as src:

        with WarpedVRT(src, resampling=0, crs=dst_proj, warp_mem_limit=5000,
                       warp_extras={'NUM_THREADS': 4}) as vrt:
            native_res = vrt.transform.to_gdal()[1]
            chunks = {'y': src.profile['blockysize'] * 10, 'x': src.profile['blockxsize'] * 10}

            with rioxarray.open_rasterio(vrt, chunks=chunks,).to_dataset(name=var_name).squeeze().reset_coords(['band'],
                                                                                                             drop=True) as dst:
                dst[var_name].rio.write_nodata(vrt.meta['nodata'], inplace=True)
                dst[var_name].where(dst[var_name] == dst[var_name].rio.nodata, -1)
                dst[var_name] = (dst[var_name] * 100).astype('i1')
                dst[var_name].rio.write_nodata(-1, inplace=True)

                # dst.load()
                print(dst)
                for level in levels:
                    lkey = str(level)
                    r = zooml2resmeters(zoom=level)
                    f = r / native_res
                    intf = int(round(f))



                    if f < 1:

                        # dds = dst
                        # print(level, native_res, native_res, dds[var_name].shape)
                        break
                    else:

                        dds = dst.coarsen(x=intf, boundary='trim').mean().coarsen(y=intf, boundary='trim').mean().astype('i1')
                        dds = dds.chunk({'y': src.profile['blockysize'], 'x': src.profile['blockxsize']})
                        dds[var_name].encoding.clear()
                        dds[var_name].encoding["compressor"] = compressor
                        print(level, r, dds[var_name].shape)

                        pyramid[lkey] = dds

                        #oname = os.path.join('zarr', f'{level}')

                        # dds.to_zarr(oname, consolidated=True)
                        #logger.info(f'Exported {oname}')
                        #del dds

    #print(json.dumps(pyramid.root.at))

    #pyramid.attrs = attrs


    # write the pyramid to zarr
    pyramid.to_zarr(f'./zarr/{var_name}', consolidated=True,)


def set_azure_env(sas_url: str=None, gdal_lib=gdal):
    """
    Set up the GDAL required azure env vars
    :param sas_url:
    :param gdal_lib:
    :return:
    """

    gdal_version = gdal_lib.__version__
    gdal_major_version, gdal_minor_version, _ = map(int, gdal_version.split('.'))
    if gdal_major_version<3:
        raise Exception(f'Unsupported GDAL version {gdal_major_version} {gdal_version}')

    parsed = urlparse(sas_url)
    # AZURE_SAS and AZURE_STORAGE_SAS_TOKEN
    azure_storage_account = parsed.netloc.split('.')[0]
    os.environ['AZURE_STORAGE_ACCOUNT'] = azure_storage_account
    azure_sas_token = parsed.query
    if gdal_minor_version >=5:
        os.environ['AZURE_STORAGE_SAS_TOKEN'] = azure_sas_token
    elif gdal_minor_version >=2:
        os.environ['AZURE_SAS'] = azure_sas_token

