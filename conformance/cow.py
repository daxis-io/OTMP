#!/usr/bin/env python3
"""Independent deterministic COW fixtures and reconstruction for crash checks.

The writer emits only uncompressed pages. Runtime zstd qualification lives in Rust.
Fixture construction may compare full retained images; ordinary writers must not.
"""
import argparse
import copy
import hashlib
import json
import pathlib
import struct

ROOT = pathlib.Path(__file__).resolve().parent.parent


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def digest(data):
    return 'sha256:' + hashlib.sha256(data).hexdigest()


def cbor(value):
    def head(major, number):
        if number < 24:
            return bytes([major * 32 + number])
        for extra, size in ((24, 1), (25, 2), (26, 4), (27, 8)):
            if number < 1 << (size * 8):
                return bytes([major * 32 + extra]) + number.to_bytes(size, 'big')
        raise ValueError('integer overflow')
    if isinstance(value, int):
        return head(0, value)
    if isinstance(value, str):
        raw = value.encode()
        return head(3, len(raw)) + raw
    if isinstance(value, bytes):
        return head(2, len(value)) + value
    if isinstance(value, list):
        return head(4, len(value)) + b''.join(map(cbor, value))
    if isinstance(value, dict):
        entries = sorted((cbor(k), cbor(v)) for k, v in value.items())
        return head(5, len(entries)) + b''.join(k + v for k, v in entries)
    raise ValueError('unsupported CBOR value')


def decode(raw):
    cursor = 0
    def item(depth=0):
        nonlocal cursor
        assert depth < 16 and cursor < len(raw)
        major, extra = raw[cursor] >> 5, raw[cursor] & 31
        cursor += 1
        number = extra
        if extra >= 24:
            assert extra in (24, 25, 26, 27)
            size = 1 << (extra - 24)
            assert cursor + size <= len(raw)
            number = int.from_bytes(raw[cursor:cursor + size], 'big')
            cursor += size
        if major == 0:
            return number
        assert number <= len(raw)
        if major in (2, 3):
            assert cursor + number <= len(raw)
            value = raw[cursor:cursor + number]
            cursor += number
            return value if major == 2 else value.decode()
        if major == 4:
            return [item(depth + 1) for _ in range(number)]
        assert major == 5
        result = {}
        for _ in range(number):
            key = item(depth + 1)
            assert key not in result
            result[key] = item(depth + 1)
        return result
    result = item()
    assert cursor == len(raw) and cbor(result) == raw
    return result


def verified(root, reference):
    data = (root / reference['uri']).read_bytes()
    expected = reference['sha256']
    if isinstance(expected, bytes):
        expected = 'sha256:' + expected.hex()
    assert digest(data) == expected
    if 'length' in reference:
        assert len(data) == int(reference['length'])
    return data


def resolve(root, generation=None):
    if generation is None:
        head = json.loads((root / '_otmp/HEAD').read_bytes())
        generation = json.loads(verified(root, head['metadata_generation']))
    image = generation['metadata_image']
    result = bytearray(verified(root, image['checkpoint']))
    page_size, count = image['page_size'], int(image['page_count'])
    original_pages = len(result) // page_size
    result = result[:count * page_size]
    if len(result) < count * page_size:
        result.extend(bytes(count * page_size - len(result)))
    pending = [image['page_map']] if image['page_map'] else []
    seen = set()
    while pending:
        node = decode(verified(root, pending.pop()))
        if node['node_type'] == 'internal':
            pending.extend(e['child'] for e in node['entries'])
            continue
        for entry in node['entries']:
            number = entry['page_number']
            assert number not in seen and 0 < number <= count
            seen.add(number)
            pack = verified(root, entry['pack'])
            assert pack[:8] == b'OTMPPGPK' and entry['codec'] == 'none'
            offset, length = entry['offset'], entry['stored_length']
            page = pack[offset:offset + length]
            assert len(page) == page_size and hashlib.sha256(page).digest() == entry['page_sha256']
            result[(number - 1) * page_size:number * page_size] = page
    assert all(p in seen for p in range(original_pages + 1, count + 1))
    return bytes(result)


def fixture_files():
    source = ROOT / 'conformance/tables/transactions'
    files = {}
    generations = [json.loads(next((source / f'_otmp/generations/{v}').glob('*.json')).read_bytes()) for v in range(3)]
    source_head = json.loads((source / '_otmp/HEAD').read_bytes())
    for folder in ('data', '_otmp/commits/0', '_otmp/commits/1', '_otmp/commits/2'):
        for path in (source / folder).rglob('*'):
            if path.is_file():
                files[str(path.relative_to(source))] = path.read_bytes()
    base = generations[0]['metadata_image']['checkpoint']
    parent_image = verified(source, base)
    files[base['uri']] = parent_image
    mappings = {}
    parent_reference = None
    for version, original in enumerate(generations):
        generation = copy.deepcopy(original)
        image = generation['metadata_image']
        current = verified(source, image['checkpoint'])
        if version:
            changed = [(p + 1, current[p * 4096:(p + 1) * 4096]) for p in range(len(current) // 4096)
                       if current[p * 4096:(p + 1) * 4096] != parent_image[p * 4096:(p + 1) * 4096]]
            payload_offset = 64 + 64 * len(changed)
            header = struct.pack('>8sHHIIIQQ24x', b'OTMPPGPK', 1, 0, 0, 4096, len(changed), 64, payload_offset)
            entries = []
            index = b''
            for i, (number, page) in enumerate(changed):
                offset = payload_offset + i * 4096
                hashed = hashlib.sha256(page).digest()
                index += struct.pack('>QQIIB7x32s', number, offset, 4096, 4096, 0, hashed)
                entries.append(dict(page_number=number, offset=offset, stored_length=4096, raw_length=4096, codec='none', page_sha256=hashed))
            pack = header + index + b''.join(page for _, page in changed)
            pack_uri = '_otmp/page-packs/' + hashlib.sha256(pack).hexdigest() + '.otmppg'
            files[pack_uri] = pack
            for entry in entries:
                entry['pack'] = dict(uri=pack_uri, sha256=hashlib.sha256(pack).digest(), length=len(pack))
                mappings[entry['page_number']] = entry
            mappings = {p: e for p, e in mappings.items() if p <= len(current) // 4096}
            node = cbor(dict(version=1, node_type='leaf', entries=[mappings[p] for p in sorted(mappings)]))
            assert len(mappings) <= 128
            node_uri = '_otmp/page-maps/' + hashlib.sha256(node).hexdigest() + '.cbor'
            files[node_uri] = node
            image['checkpoint'] = base
            image['page_map'] = dict(uri=node_uri, sha256=digest(node), length=str(len(node)), height=0)
            raw_id = bytes.fromhex(generation['table_id'].replace('-', ''))
            image['image_root_sha256'] = digest(b'OTMP-SQLITE-IMAGE\0' + raw_id + struct.pack('>QIQ', version, 4096, int(image['page_count'])) + bytes.fromhex(base['sha256'][7:]) + hashlib.sha256(node).digest())
            generation['physical_parent'] = parent_reference
        data = canonical(generation)
        uri = f"_otmp/generations/{version}/{generation['generation_id']}.json"
        files[uri] = data
        parent_reference = dict(uri=uri, sha256=digest(data), length=str(len(data)), media_type='application/vnd.otmp.generation+json')
        parent_image = current
    head = copy.deepcopy(source_head)
    head.update(table_version='2', root_revision='2', semantic_state_sha256=generation['semantic_state_sha256'], semantic_commit=generation['semantic_commit'], metadata_generation=parent_reference)
    files['_otmp/HEAD'] = canonical(head)
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--resolve', type=pathlib.Path)
    parser.add_argument('--output', type=pathlib.Path)
    args = parser.parse_args()
    if args.resolve:
        args.output.write_bytes(resolve(args.resolve))
        return
    target = ROOT / 'conformance/tables/incremental'
    files = fixture_files()
    if args.check:
        actual = {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file()}
        assert files == actual, 'incremental fixture regeneration differs'
    else:
        for uri, data in files.items():
            path = target / uri
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
    for version in range(3):
        generation = json.loads(next((target / f'_otmp/generations/{version}').glob('*.json')).read_bytes())
        expected_root = ROOT / f'conformance/tables/transactions/_otmp/checkpoints/{version}'
        assert resolve(target, generation) == next(expected_root.glob('*.sqlite3')).read_bytes()
    print('incremental fixture: versions 0-2 reconstruct exact retained SQLite bytes')


if __name__ == '__main__':
    main()
