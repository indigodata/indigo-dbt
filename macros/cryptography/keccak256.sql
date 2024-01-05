{% macro func_keccak256() -%}
    create or replace function {{ target.schema }}.keccak256(data string)
    returns varchar
    language python
    runtime_version = '3.8'
    packages = ('pycryptodome')
    handler = 'keccak_hash'
    as
    $$
    from Crypto.Hash import keccak
    def keccak_hash(data):
        long_id = bytes.fromhex(data)
        k = keccak.new(digest_bits=256)
        k.update(long_id)
        return k.hexdigest()
    $$;
{%- endmacro %}