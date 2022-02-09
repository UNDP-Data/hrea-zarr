from pipeline import util
import logging
from osgeo import gdal, ogr
import os
import dotenv
from azure.storage.blob.aio import ContainerClient
import itertools
from tqdm import tqdm
from urllib.parse import urlparse
dotenv.load_dotenv()
gdal.UseExceptions()

#sas_url='https://undpngddlsgeohubdev01.blob.core.windows.net/sids?sv=2020-10-02&st=2022-01-21T23%3A34%3A47Z&se=2025-01-22T23%3A34%3A00Z&sr=c&sp=racwdxlt&sig=6l8jlQ0a%2FLhs3410YYxtgY56mCuUusFb2IY5yxE2WnU%3D'



def scantree(path):
    """Recursively yield sorted DirEntry objects for given directory."""
    for entry in sorted(os.scandir(path), key=lambda entry: entry.name):
        if entry.is_dir(follow_symlinks=False):
            #yield entry
            yield from scantree(entry.path)
        else:
            yield entry

def mkdir_recursive(path):
    """
        make dirs in the path recursively
        :param path:
        :return:
    """

    sub_path = os.path.dirname(path)
    if not os.path.exists(sub_path):
        mkdir_recursive(sub_path)
    if not os.path.exists(path):
        os.mkdir(path)

def slicer(iterable=None, n=None):
    """
    Chunked access by n items to an iterator
    :param n:
    :param iterable:
    :return:
    """
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk

def count(iter):
    try:
        return len(iter)
    except TypeError:
        return sum(1 for _ in iter)



async def upload_file(container_client_instance=None, src=None, dst_blob_name=None,  overwrite=False, max_concurrency=8):

    """
    Async upload a local file to Azure container.
    Do not use directly. This function is meant to be used inside a loop where many files
    are uploaded asynchronously
    :param container_client_instance: instance of azure.storage.blob.aio.ContainerClient
    :param src: str, the path of the file
    :param dst_blob_name: str, the name of the uploaded blob. The file content will be stored in AZ under this name
    :param overwrite: bool, default=False, flag to force uploading an existing file
    :param max_concurrency, default = 8, maximum number of parallel connections to use when the blob size exceeds 64MB

    :return: None
    """

    parsed_src_url = urlparse(src)

    if not dst_blob_name:
        _, dst_blob_name = os.path.split(parsed_src_url.path)

    assert dst_blob_name not in [None, '', ' '], f'Invalid destination blob name {dst_blob_name}'


    with open(src, 'rb') as data:
        blob_client = await container_client_instance.upload_blob(name=dst_blob_name, data=data,
                                                        blob_type='BlockBlob', overwrite=overwrite,
                                                        max_concurrency=max_concurrency)
        logger.debug(f'{src} was uploaded as {dst_blob_name}')
        return blob_client, src





async def folder2azureblob(container_client_instance=None, src_folder=None, dst_blob_name=None,
                            overwrite=False, max_concurrency=8, timeout=None
                           ):
    """
    Asynchronously upload a local folder (including its content) to Azure blob container

    :param container_client_instance: instance of azure.storage.blob.aio.ContainerClient
    :param src_folder: str, full abs path to the folder to be uploaded
    :param dst_blob_name: str the name of the blob where the content fo the folder will be downloaded
    :param overwrite: bool, defaults to false, sepcifiy if an existing blob will be overwritten
    :param max_concurrency: int, maximum number of parallel connections to use when the blob size exceeds 64MB
    :param timeout, timeout in seconds to be applied to uploading all files in the folder.
    :return:
    """
    assert src_folder not in [None, '', '/' ], f'src_folder={src_folder} is invalid'
    assert os.path.exists(src_folder), f'src_folder={src_folder} does not exist'
    assert os.path.isabs(src_folder), f'src_folder={src_folder} is not a an absolute path'
    assert os.path.isdir(src_folder), f'src_folder={src_folder} is not a directory'
    assert len(src_folder)>1, f'src_folder={src_folder} is invalid'



    try:
        async with container_client_instance:
            prefix = os.path.split(src_folder)[-1] if dst_blob_name is None else dst_blob_name
            r = scantree(src_folder)
            nfiles = count(r)
            nchunks = nfiles//100 + 1
            n = 0
            r = scantree(src_folder)
            with tqdm(total=nchunks, desc="Uploading ... ", initial=0, unit_scale=True,
                      colour='green') as pbar:
                for chunk in slicer(r,100):
                    ftrs = list()
                    #logger.info(f'Uploading file chunk no {n} from {nchunks} - {n / nchunks * 100:.2f}%')
                    await asyncio.sleep(1)
                    for local_file in chunk:

                        if not local_file.is_file():continue
                        blob_path = os.path.join(prefix, os.path.relpath(local_file.path, src_folder))
                        #print(e.path, blob_path)
                        fut = asyncio.ensure_future(
                            upload_file(container_client_instance=container_client_instance,
                                                src=local_file.path, dst_blob_name=blob_path, overwrite=overwrite,
                                                max_concurrency=max_concurrency)
                        )
                        ftrs.append(fut)



                    done, pending = await asyncio.wait(ftrs, timeout=timeout, return_when=asyncio.ALL_COMPLETED)
                    results = await asyncio.gather(*done, return_exceptions=True)
                    for res in results:
                        if type(res) == tuple:
                            blob_client, file_path_to_upload = res
                        else: #error
                            logger.error(f'{dst_blob_name} was not uploaded successfully')
                            logger.error(res)

                    for failed in pending:
                        blob_client, file_path_to_upload =  await failed
                        logger.debug(f'Uploading {file_path_to_upload} to {container_client_instance.url} has timed out.')
                    pbar.update(1)
                    n+=1
    except Exception as err:
        logger.error(f'Failed to upload {src_folder} to {container_client_instance.url}')
        raise




async def upload_zarr(sas_url=None, src_folder=None, dst_blob_name=None, timeout=30*60):
    """
    Asyn upload a folder to Azure blob
    :param sas_url: str, the SAS url
    :param src_folder: str, full abs path to the folder
    :param dst_blob_name: str, relative (to container) timeout of the
    :param timeout:
    :return:
    """

    async with ContainerClient.from_container_url(sas_url) as cc:
        return await folder2azureblob(
            container_client_instance=cc,
            src_folder=src_folder,
            dst_blob_name=dst_blob_name,
            overwrite=True,
            timeout=timeout
        )





if __name__ == '__main__':
    import asyncio
    from urllib.parse import urlencode
    logger = logging.getLogger()
    logger.setLevel('INFO')

    azlogger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
    azlogger.setLevel(logging.WARNING)

    sas_url = os.environ['HREA_SAS_URL']
    logging.basicConfig()

    parsed = urlparse(sas_url)


    country = 'Kenya'
    varname='lightscore'
    year = None

    if True:
        #set azure env vars as per GDLA versions
        util.set_azure_env(sas_url=sas_url, gdal_lib=gdal)

        cogs = asyncio.run(
            util.list_az_cogs(
                hrea_sas_url=sas_url,
                country=country,
                year=year,
                var_name=varname,
                for_gdal=True,
                for_cogeo=True
            )
        )
        from osgeo import gdal


        VRT = f'Kenya_set_lightscore.vrt'
        #print(parsed.query)
        #print(urlencode(parsed.query))
        blob_full_path = f"{os.path.join(f'{parsed.scheme}://{parsed.netloc}{parsed.path}', VRT)}?{parsed.query}"
        print(f'https://undp.livedata.link/hrea/info?url={blob_full_path}')
        #gdal.BuildVRT(VRT, cogs, separate=True, callback=gdal.TermProgress_nocb)
        a = gdal.OpenEx(VRT, 1)
        b = a.GetRasterBand(1)
        print(b)
        print(b.GetDescription())
        #b.SetDescription('2012')
        del b
        del a

        for c in cogs:
            print(c)
        exit()
    else:
        cogs = ['/work/py/hrea-zarr/pipeline/Kenya_set_lightscore_sy_2013.tif']



    if cogs:

        for cog in cogs:
            r = util.cog2zarr(src_path=cog, var_name=varname, levels=5)
            # asyncio.run(
            #     upload_zarr(
            #         sas_url=sas_url,
            #         src_folder='/work/py/hrea-zarr/pipeline/zarr',
            #         dst_blob_name='zarr/Kenya',
            #         timeout=3 * 60 * 60  # three hours
            #     )
            # )

    else:
        logger.info(f'No COG files were found for variable: {varname}, country:{country} and year {year}')

