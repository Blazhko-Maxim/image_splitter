import gc
import glob
import os
import re
import shutil
from pathlib import Path
import boto3
import sys
from osgeo import gdal
from shapely.wkt import loads
from tqdm import tqdm


if len(sys.argv) != 3:
    print("Missing arguments")
    sys.exit()

FILE_KEY = sys.argv[1]
DESTINATION = sys.argv[2]
RASTER_EXTENSION = '.tif'
RESULT_NAME = 'splitter'
SPLITTING_SIZE = 1024
PREFIX = 'updated'
BUCKET = 'python-pictures-bucket'

RASTERS_SOURCES = [
    DESTINATION + RESULT_NAME,
    # list because one big raster can be into different folders
    # no trailing slash
]
RASTERS_DESTINATION = DESTINATION + 'splitted-image'

s3 = boto3.client("s3")
result = s3.list_objects_v2(Bucket=BUCKET, Prefix=FILE_KEY)
if 'Contents' in result:
    print("Downloading file...")
else:
    print("No such file or bucket")
    sys.exit()

Path(DESTINATION).mkdir(parents=True, exist_ok=True)
object_size = result['Contents'][0]['Size']
with tqdm(total=object_size, unit='B', unit_scale=True, desc=FILE_KEY) as progress:
    s3.download_file(Bucket=BUCKET, Key=FILE_KEY, Filename=DESTINATION + RESULT_NAME + ".ecw", Callback=lambda bytes_transferred: progress.update(bytes_transferred),)
COMMAND = 'docker run --rm -i --name gdalecw -v ' + DESTINATION + \
          ':/home/datafolder ginetto/gdal:2.4.4_ECW gdal_translate /home/datafolder/' + RESULT_NAME + \
          '.ecw /home/datafolder/' + RESULT_NAME + RASTER_EXTENSION
os.system(COMMAND)


def list_all_clip(input_raster: str, clip_output_folder: str, splitting_size: int, name: str) -> list:
    """
    prepare raster for clipping in requested splitting size.
    splitting_size: size of clipped image (by pixels)
    name: file name
    """
    all_clip = []
    data = gdal.Open(input_raster)
    band = data.GetRasterBand(1)
    if band.XSize == splitting_size and band.YSize == splitting_size:
        # if raster is already 1024, just move it to the output folder
        shutil.move(input_raster, os.path.join(clip_output_folder, os.path.basename(input_raster)))
    else:
        Projection = data.GetProjectionRef()
        geoTransform = data.GetGeoTransform()
        PixelRes = geoTransform[1]
        xmin = float(geoTransform[0])
        ymax = float(geoTransform[3])
        xmax = float(xmin + geoTransform[1] * data.RasterXSize)
        ymin = float(ymax + geoTransform[5] * data.RasterYSize)
        gridHeight = float(geoTransform[1] * splitting_size)
        gridWidth = float(abs(geoTransform[5] * splitting_size))
        rows = round((ymax - ymin) / gridHeight)
        cols = round((xmax - xmin) / gridWidth)
        ringXleftOrigin = xmin
        ringXrightOrigin = xmin + gridWidth
        ringYtopOrigin = ymax
        ringYbottomOrigin = ymax - gridHeight
        countcols = 0
        for countcols in tqdm(range(cols)):
        #while countcols < cols:
            countcols += 1
            ringYtop = ringYtopOrigin
            ringYbottom = ringYbottomOrigin
            countrows = 0
            while countrows < rows:
                countrows += 1
                wkt = f'POLYGON (({ringXleftOrigin} {ringYtop}, {ringXrightOrigin} {ringYtop}, {ringXrightOrigin} {ringYbottom}, {ringXleftOrigin} {ringYbottom}, {ringXleftOrigin} {ringYtop}))'
                bounds = loads(wkt).bounds
                box = "_".join(map(str, bounds))
                OutTileImage = os.path.join(clip_output_folder, f'{name}_{box}_.tiff')
                all_clip.append(
                    [input_raster, OutTileImage, [ringXleftOrigin, ringYbottom, ringXrightOrigin, ringYtop], PixelRes,
                     Projection])
                ringYtop = ringYtop - gridHeight
                ringYbottom = ringYbottom - gridHeight
            ringXleftOrigin = ringXleftOrigin + gridWidth
            ringXrightOrigin = ringXrightOrigin + gridWidth
        gc.collect()
    return all_clip


def clip(all_clip: list) -> None:
    gdal.Warp(all_clip[1], all_clip[0], format='GTiff', outputBounds=all_clip[2], xRes=all_clip[3], yRes=all_clip[3],
              dstSRS=all_clip[4], warpOptions=['-wo NUM_THREADS=ALL_CPUS -multi'])
    gc.enable()


def clip_raster(input_raster: str) -> None:
    regex = re.compile('[^a-zA-Z]')
    name = regex.sub('', PREFIX)
    all_clip = list_all_clip(input_raster, RASTERS_DESTINATION, SPLITTING_SIZE, name)
    print("Start clipping")
    for i in tqdm(all_clip, total=len(all_clip)):
         clip(i)


def run() -> None:
    shutil.rmtree(RASTERS_DESTINATION, ignore_errors=True)
    Path(RASTERS_DESTINATION).mkdir(parents=True, exist_ok=False)
    for rasters_source in RASTERS_SOURCES:
        print('*' * 50)
        print(f'Process {rasters_source} into {RASTERS_DESTINATION}')
        print(rasters_source + RASTER_EXTENSION)
        rasters = glob.glob(rasters_source + RASTER_EXTENSION)
        for r in rasters:
            clip_raster(r)


if __name__ == '__main__':
    run()
    print('Done')