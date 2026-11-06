#!/usr/bin/env python3
"""Exercise real Linux cache residency without clearing caches for other files."""
import importlib.util
import os
from pathlib import Path
import tempfile

spec = importlib.util.spec_from_file_location('warm_cache', Path(__file__).with_name('warm-cache.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
Path('build/warm-cache').mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory(prefix='sparkbench-cache-test-', dir='build/warm-cache') as directory:
    root = Path(directory).resolve()
    for partition in ('win%2Fregistry_value_activity', 'with space', 'literal%20value'):
        file = root / ('eventtype=' + partition) / 'input.parquet'
        file.parent.mkdir()
        file.write_bytes(b'data')
        mounted = Path('/warehouse') / file.relative_to(root)
        for uri in ('file:' + str(mounted), mounted.as_uri()):
            assert module.resolve_input({'path': uri, 'bytes': 4}, Path('/warehouse'), root) == file
    for bad in ('file:/warehouse/../outside', 'file:/warehouse/%2E%2E/outside', 's3://bucket/key'):
        try:
            module.resolve_input({'path': bad, 'bytes': 4}, Path('/warehouse'), root)
            raise AssertionError('Unsafe path accepted')
        except ValueError:
            pass
    path = root / 'input.parquet'
    path.write_bytes(b'x' * (8 * 1024 * 1024))
    with path.open('rb') as file:
        os.fsync(file.fileno())
        os.posix_fadvise(file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    cold, pages = module.residency(path)
    assert cold < pages, 'mincore did not distinguish uncached pages'
    result = module.warm([path], .99)
    assert result['residentFraction'] >= .99
    assert result['inputBytes'] == result['bytesRead'] == 8 * 1024 * 1024
    assert result['residentFractionBefore'] < result['residentFraction']
print('Warm-cache test passed: own-file cache eviction, real residency measurement, full read, >=99% resident.')
