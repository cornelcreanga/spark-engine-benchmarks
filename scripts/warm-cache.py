#!/usr/bin/env python3
"""Warm shared local files on the Minikube node, then measure initial page residency."""
import argparse
import concurrent.futures
import ctypes
import json
import mmap
import os
from pathlib import Path
import time
from urllib.parse import urlparse, unquote


def residency(path):
    size = path.stat().st_size
    if not size:
        return 0, 0
    pages = (size + mmap.PAGESIZE - 1) // mmap.PAGESIZE
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_ubyte)]
    libc.mincore.restype = ctypes.c_int
    with path.open('rb') as source:
        with mmap.mmap(source.fileno(), size, flags=mmap.MAP_PRIVATE,
                       prot=mmap.PROT_READ | mmap.PROT_WRITE) as mapping:
            address = ctypes.c_char.from_buffer(mapping)
            vector = (ctypes.c_ubyte * pages)()
            try:
                if libc.mincore(ctypes.addressof(address), size, vector):
                    raise OSError(ctypes.get_errno(), 'mincore failed')
                resident = sum(value & 1 for value in vector)
            finally:
                del address
    return resident, pages


def read_file(path):
    total = 0
    with path.open('rb', buffering=0) as source:
        while True:
            data = source.read(4 * 1024 * 1024)
            if not data:
                return total
            total += len(data)


def warm(paths, minimum, passes=3):
    started = time.monotonic()
    before = [residency(p) for p in paths]
    total_pages = sum(total for _, total in before)
    if not total_pages:
        raise RuntimeError('No input pages to warm')
    bytes_read = 0
    for attempt in range(1, passes + 1):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            bytes_read += sum(pool.map(read_file, paths))
        counts = [residency(p) for p in paths]
        resident = sum(count for count, _ in counts)
        fraction = resident / total_pages
        if fraction >= minimum:
            return {'files': len(paths), 'inputBytes': sum(p.stat().st_size for p in paths),
                    'bytesRead': bytes_read, 'passes': attempt,
                    'residentFractionBefore': sum(n for n, _ in before) / total_pages,
                    'residentFraction': fraction, 'residentPages': resident,
                    'totalPages': total_pages, 'seconds': time.monotonic() - started,
                    'checkedAtEpochMillis': int(time.time() * 1000)}
    raise RuntimeError(f'Input cache residency {fraction:.4%} is below {minimum:.2%} after {passes} passes')


def resolve_input(item, mount, root):
    uri = urlparse(item['path'])
    if uri.scheme != 'file' or uri.netloc not in ('', 'localhost'):
        raise ValueError('Warm-cache mode requires local file URIs')
    # Hadoop DataFile paths already contain literal partition escapes (e.g. win%2F...).
    # Try that filesystem spelling first; URI-encoded paths may require one decode.
    for spelling in dict.fromkeys((uri.path, unquote(uri.path))):
        try:
            path = (root / Path(spelling).relative_to(mount)).resolve()
        except ValueError:
            continue
        if not path.is_relative_to(root):
            continue
        if path.is_file():
            if path.stat().st_size != item['bytes']:
                raise ValueError('Input length does not match warm-up request')
            return path
    raise ValueError('Input path is missing or outside the shared volume: ' + item['path'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', required=True)
    parser.add_argument('--mount', required=True)
    parser.add_argument('--volume-root', required=True)
    parser.add_argument('--wait', type=int, default=600)
    parser.add_argument('--min-resident', type=float, default=.99)
    args = parser.parse_args()
    directory = Path(args.directory)
    try:
        deadline = time.monotonic() + args.wait
        request = directory / 'request.json'
        while not request.is_file():
            if time.monotonic() >= deadline:
                raise TimeoutError('Timed out waiting for driver warm-up request')
            time.sleep(.5)
        files = json.loads(request.read_text())
        mount = Path(args.mount)
        root = Path(args.volume_root).resolve()
        paths = []
        for item in files:
            paths.append(resolve_input(item, mount, root))
        result = warm(paths, args.min_resident)
        (directory / 'report.json').write_text(json.dumps(result, indent=2) + '\n')
        temporary = directory / 'done.tmp'
        temporary.write_text(''.join(f'{key}={value}\n' for key, value in result.items()))
        temporary.replace(directory / 'done.properties')
        print(json.dumps(result), flush=True)
    except Exception as error:
        if directory.is_dir():
            (directory / 'error.txt').write_text(str(error))
        raise


if __name__ == '__main__':
    main()
