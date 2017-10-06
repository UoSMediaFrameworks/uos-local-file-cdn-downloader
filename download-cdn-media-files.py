
import json
import requests
import os
from tqdm import tqdm
import logging
import urllib

logging.basicConfig(filename='./download_errors.log', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

videoIngest = 'ingest-files/video-media-objects-for-download.json'
imageIngest = 'ingest-files/image-media-objects-for-download.json'

azure_blob_url = 'https://uosassetstore.blob.core.windows.net/assetstoredev/'

download_images = False
download_videos = True

def ensure_cdn_directories_are_created():
    # APEP ensure root directory is created, folders of image ids go directly in here
    if not os.path.isdir('cdn'):
        os.mkdir('cdn')

    if not os.path.isdir('cdn/video'):
        os.mkdir('cdn/video')

    if not os.path.isdir('cdn/video/raw'):
        os.mkdir('cdn/video/raw')

    if not os.path.isdir('cdn/video/transcoded'):
        os.mkdir('cdn/video/transcoded')

    if not os.path.isdir('cdn/video/transcoded/dash'):
        os.mkdir('cdn/video/transcoded/dash')

# APEP ensure the the cdn directories are created
ensure_cdn_directories_are_created()


def download_image_media_file_and_store(imoJson):
    print("download_image_media_file_and_store")
    image_url = imoJson['image']['url']
    image_url = urllib.unquote(image_url)

    # 1. strip https://uosassetstore.blob.core.windows.net/assetstoredev/ and include cdn path
    image_file_path = 'cdn/' + image_url.replace(azure_blob_url, '')

    # 1.01 due to legacy, some image urls are constructed poorly and require the usage of the url rather than id
    image_dir = image_url.replace(azure_blob_url, '').split("/")[0]

    # 1.1 ensure our cdn directory has the media asset folder for id
    if not os.path.isdir('cdn/' + image_dir):
        os.mkdir('cdn/' + image_dir)

    # TODO 2. check file system for remaining path (relative)
    # TODO 3. if file exists compare file sizes

    # 4. download to the relative path if need
    try:
        download_media_file(image_url, image_file_path)
    except Exception as e:
        logger.error(e)
        logger.info(imoJson)


def download_media_file(url, local_cdn_file_path, ignore_errors=False):
    # print("Downloading media file from {} to local cdn location {}".format(url, local_cdn_file_path))
    media_file_download_r = requests.get(url, stream=True)
    if ignore_errors:
        media_file_download_r.raise_for_status()
    with open(local_cdn_file_path, 'wb') as rawOutput:
        for data in tqdm(media_file_download_r.iter_content(chunk_size=1024)):
            rawOutput.write(data)


def download_video_media_object_and_store(vmo_json):
    print("download_video_media_object_and_store")
    raw_video_url = vmo_json['video']['url']
    raw_video_url = urllib.unquote(raw_video_url)

    # 1. strip https://uosassetstore.blob.core.windows.net/assetstoredev/video/raw/
    raw_video_file_path = 'cdn/' + raw_video_url.replace(azure_blob_url, '')

    # 1.1 ensure our cdn directory has the video raw asset folder for id
    if not os.path.isdir('cdn/video/raw/' + vmo_json['_id']):
        os.mkdir('cdn/video/raw/' + vmo_json['_id'])

    # 1.2 ensure our cdn directory has the video transcoded asset folder for id
    if not os.path.isdir('cdn/video/transcoded/dash/' + vmo_json['_id']):
            os.mkdir('cdn/video/transcoded/dash/' + vmo_json['_id'])

    # TODO 2. check file system for remaining path (relative)
    # TODO 3. if file exists compare

    # 4. download to relative path
    download_media_file(raw_video_url, raw_video_file_path)

    # 5. expand urls to include all the transcoded assets
    audio_url       = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/audio_128k.mp4"
    audio_url_aws   = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/audio-final.webm"
    video_600k_url  = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/video_600k.mp4"
    video_560k_url  = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/video_512x384_560k-final.webm"
    video_1200k_url = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/video_1200k.mp4"
    video_1050k_url = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/video_640x480_1050k-final.webm"
    video_2400k_url = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/video_2400k.mp4"
    video_2350k_url = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/video_1280x720_2350k-final.webm"
    video_4800k_url = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/video_4800k.mp4"
    video_4300k_url = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/video_1920x1080_4300k-final.webm"
    video_manifest_url = azure_blob_url + "video/transcoded/dash/" + vmo_json["_id"] + "/video_manifest.mpd"

    # 5.1 generate local cdn file paths
    audio_file_path       = "cdn/video/transcoded/dash/" + vmo_json["_id"] + "/audio_128k.mp4"
    audio_file_path_aws   = "cdn/video/transcoded/dash/" + vmo_json["_id"] + "/audio-final.webm"
    video_600k_file_path  = "cdn/video/transcoded/dash/" + vmo_json["_id"] + "/video_600k.mp4"
    video_560k_file_path  = "cdn/video/transcoded/dash/" + vmo_json["_id"] + "/video_512x384_560k-final.webm"
    video_1200k_file_path = "cdn/video/transcoded/dash/" + vmo_json["_id"] + "/video_1200k.mp4"
    video_1050k_file_path = "cdn/video/transcoded/dash/" + vmo_json["_id"] + "/video_640x480_1050k-final.webm"
    video_2400k_file_path = "cdn/video/transcoded/dash/" + vmo_json["_id"] + "/video_2400k.mp4"
    video_2350k_file_path = "cdn/video/transcoded/dash/" + vmo_json["_id"] + "/video_1280x720_2350k-final.webm"
    video_4800k_file_path = "cdn/video/transcoded/dash/" + vmo_json["_id"] + "/video_4800k.mp4"
    video_4300k_file_path = "cdn/video/transcoded/dash/" + vmo_json["_id"] + "/video_1920x1080_4300k-final.webm"
    video_manifest_file_path = "cdn/" + "video/transcoded/dash/" + vmo_json["_id"] + "/video_manifest.mpd"

    download_media_file(audio_url, audio_file_path)
    download_media_file(audio_url_aws, audio_file_path_aws)
    download_media_file(video_600k_url, video_600k_file_path)
    download_media_file(video_560k_url, video_560k_file_path)
    download_media_file(video_1200k_url, video_1200k_file_path)
    download_media_file(video_1050k_url, video_1050k_file_path)
    download_media_file(video_2400k_url, video_2400k_file_path)
    download_media_file(video_2350k_url, video_2350k_file_path)
    download_media_file(video_4800k_url, video_4800k_file_path)
    download_media_file(video_4300k_url, video_4300k_file_path)
    download_media_file(video_manifest_url, video_manifest_file_path)

    # TODO 6. most likely going to need to handle 404s in case of missing audio
    #   handle potential 404 on any of them
    # TODO 7. we may have an issue as not all scenes have gone through the AWS pipeline


if download_videos:
    with open(videoIngest) as videoIngestFile:
        videoIngestData = json.load(videoIngestFile)
        for videoJson in videoIngestData:
            download_video_media_object_and_store(videoJson)

if download_images:
    with open(imageIngest) as imageIngestFile:
        imageIngestData = json.load(imageIngestFile)
        for imageJson in imageIngestData:
            download_image_media_file_and_store(imageJson)
