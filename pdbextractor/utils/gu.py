import json
from datetime import datetime

import oyaml as yaml
import os
from pathlib import Path
from typing import List, Iterable, Iterator, Dict
from config.config import getoptions

options = getoptions()
logger = options['logger']


def dstd(sdate):
    f = "%Y%m%d"
    result = datetime.now().strftime(f)
    try:
        if isinstance(sdate, str):
            result = datetime.strptime(sdate, "%Y-%m-%dT%H:%M:%SZ").strftime(f)
        if isinstance(sdate, datetime):
            result = sdate.strftime(f)
    except ValueError as e:
        pass
    return result


# Global specific utils
def get_file_info(path: str) -> Dict:
    result = {"path": path, "name": "", "parts": list(), "parents": list(), "suffixes": list()}
    if is_valid_file(path):
        result["path"] = str(Path(path).absolute())
        result["name"] = Path(path).name
        result["parts"] = list(Path(path).parts)
        result["parents"] = list(Path(path).parents)
        result['suffixes'] = [suf.strip(".") for suf in Path(path).suffixes]
    return result


def get_dirs_r(path: str, sort=False) -> Iterable[str]:
    """
    Extract files recursively start from current foder
    :param sort: Sort flag
    :param path: Directory path
    :return result: List of dirs
    """
    result = list()
    try:
        for root, subfolders, files in os.walk(path):
            for item in subfolders:
                fn = os.path.join(root, item)
                result.append(fn)
        if sort:
            result = sorted(result)
    except Exception as e:
        logger.error(e)
    return result


def get_files_r_filtered(path: str, sort=False, sufixes=None) -> Iterable[str]:
    """
        Extract files recursively start from current foder
        :param sufixes:  Filter file extensions
        :param sort: Sort flag
        :param path: Directory path
        :return result: List of files
        """
    ipvs = ["ipv4", "ipv6"]
    isos = []
    result = []
    sufixes = {".jsonl"} if sufixes is None else sufixes
    try:
        for root, subfolder, files in os.walk(path):
            for item in files:
                path = Path(os.path.join(root, item))
                if path.suffix in sufixes:
                    parents = [parent for parent in path.parents]
                    if parents[1] and parents[1].name in ipvs:
                        fn = os.path.join(root, item)
                        result.append(fn)
        if sort:
            result = sorted(result)
    except Exception as e:
        logger.error(e)
    return result


def get_files_r(path: str, sort=False, sufixes=None) -> Iterable[str]:
    """
    Extract files recursively start from current foder
    :param sufixes:  Filter file extensions
    :param sort: Sort flag
    :param path: Directory path
    :return result: List of files
    """
    result = []
    sufixes = {".jsonl"} if sufixes is None else sufixes
    try:
        for root, subfolder, files in os.walk(path):
            for item in files:
                if Path(item).suffix in sufixes:
                    fn = os.path.join(root, item)
                    result.append(fn)
        if sort:
            result = sorted(result)
    except Exception as e:
        logger.error(e)
    return result


def get_dirs(path: str, sort=False) -> Iterable[str]:
    """
       Extract directories from current directory
       :param sort: Sort flag
       :param path: Directory path
       :return result: List of dirs
    """
    result = []
    try:
        for item in os.listdir(path):
            p = os.path.join(path, item)
            if os.path.isdir(p):
                result.append(p)
        if sort:
            result = sorted(result)
    except Exception as e:
        logger.error(e)
    return result


def get_files(path: str, sort=False) -> Iterable[str]:
    """
    Extract files from current directory
    :param sort: Sort flag
    :param path: Directory path
    :return result: List of files
    """
    result = []
    try:
        for item in os.listdir(path):
            p = os.path.join(path, item)
            if os.path.isfile(p):
                result.append(p)
        if sort:
            result = sorted(result)
    except Exception as e:
        logger.error(e)
    return result


def get_dir_file_gen(root_name, ms, path) -> Iterator[dict]:
    try:
        if os.path.exists(path):
            fn = os.path.join('{dir}.{name}.{ext}'.format(dir=root_name, name=ms, ext='jsonl'))
            filepath = os.path.join(path, fn)
            if os.path.exists(filepath) and os.path.isfile(filepath):
                with open(filepath) as f:
                    for line in f:
                        data = json.loads(line.strip())
                        yield data
    except IOError:
        raise StopIteration


def is_valid_dir(source: str) -> bool:
    """
    Validate directory with source files.
    :param source: Directory name
    :return:   boolean
    """
    result = False
    try:
        if os.path.exists(source) and os.path.isdir(source)\
                and os.access(source, os.R_OK) and os.listdir(source):
            result = True
        else:
            raise IOError("Bad directory <{}>".format(source))
    except IOError as e:
        logger.error(e)
    return result


def is_valid_file(source: str) -> bool:
    """
    Validate file.
    :param source: file name
    :return:   boolean
    """
    result = False
    try:
        if os.path.exists(source) and os.path.isfile(source) and os.access(source, os.R_OK):
            result = True
        else:
            raise IOError("Bad file <{}>".format(source))
    except IOError as e:
        logger.error(e)
    return result


# Uploader speific utils
def read_file_gen(d: str) -> Iterator[dict]:
    try:
        with open(d) as f:
            for line in f:
                data = json.loads(line.strip())
                yield data
    except IOError:
        raise StopIteration


def read_file(d: str) -> Iterator[dict]:
    result = []
    try:
        with open(d) as f:
            for line in f:
                data = json.loads(line.strip())
                result.append(data)
    except IOError:
        raise StopIteration
    return result


def save_as_jsonl(data, fn):
    try:
        with open(fn, 'w', encoding='utf8') as outfile:
            for d in data:
                outfile.write(json.dumps(d))
                outfile.write("\n")
    except IOError as e:
        logger.error(e)


def save_as_json(data, fn):
    try:
        with open(fn, 'w', encoding='utf8') as outfile:
            json.dump(data, outfile)
    except IOError as e:
        logger.error(e)


def save_as_yaml(data, fn, mode="w"):
    try:
        with open(fn, mode=mode, encoding='utf8') as outfile:
            if data:
                stream = yaml.dump(data, default_flow_style=False)
                stream = stream.replace("'", '')
                outfile.write(stream.replace('\n- ', '\n\n- '))
    except IOError as e:
        logger.error(e)


def remove_file(fn):
    try:
        if os.path.exists(fn):
            os.remove(fn)
    except OSError as e:
        logger.error(e)