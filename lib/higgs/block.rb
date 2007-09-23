# = block read/write
#
# Author:: $Author$
# Date:: $Date$
# Revision:: $Revision$
#
# == license
#   :include:LICENSE
#

require 'higgs/exceptions'

module Higgs
  # block header format
  # 
  #   0..15  : Z16  : magic symbol
  #  16..19  : V    : body length
  #  20..31  : x12  : (reserved)
  #  32..33  : v    : format version
  #  34..35  : x2   : (reserved)
  #  36..37  : v    : head cksum
  #  38..39  : x2   : (reserved)
  #  40..55  : Z16  : body hash type
  #  56..57  : v    : body hash length
  #  58..71  : x14  : (reserved)
  #  73..511 : a440 : body hash binary
  #
  module Block
    # for ident(1)
    CVS_ID = '$Id$'

    include Exceptions

    class BrokenError < HiggsError
    end

    BLOCK_SIZE = 512

    FMT_VERSION = 0x00_00
    HEAD_CKSUM_BITS = 16
    HEAD_CKSUM_POS = 36..37
    HEAD_CKSUM_FMT = 'v'

    BODY_HASH = {}
    [ [ :SUM16,  proc{|s| s.sum(16).to_s           },  nil            ],
      [ :MD5,    proc{|s| Digest::MD5.digest(s)    }, 'digest/md5'    ],
      [ :RMD160, proc{|s| Digest::RMD160.digest(s) }, 'digest/rmd160' ],
      [ :SHA1,   proc{|s| Digest::SHA1.digest(s)   }, 'digest/sha1'   ],
      [ :SHA256, proc{|s| Digest::SHA256.digest(s) }, 'digest/sha2'   ],
      [ :SHA384, proc{|s| Digest::SHA384.digest(s) }, 'digest/sha2'   ],
      [ :SHA512, proc{|s| Digest::SHA512.digest(s) }, 'digest/sha2'   ]
    ].each do |hash_symbol, hash_proc, hash_lib|
      if (hash_lib) then
        begin
          require(hash_lib)
        rescue LoadError
          next
        end
      end
      BODY_HASH[hash_symbol] = hash_proc
    end

    BODY_HASH_BIN = {}
    BODY_HASH.each do |hash_symbol, hash_proc|
      BODY_HASH_BIN[hash_symbol.to_s] = hash_proc
    end

    HEAD_FMT = [
      'Z16',                    # magic symbol
      'V',                      # body length
      'x12',                    # (reserved)
      'v',                      # format version
      'x2',                     # (reserved)
      HEAD_CKSUM_FMT,           # head cksum
      'x2',                     # (reserved)
      'Z16',                    # body hash type
      'v',                      # body hash length
      'x14',                    # (reserved)
      'a440'                    # body hash binary
    ].join('')

    def padding_size(bytes)
      r = bytes % BLOCK_SIZE
      (r > 0) ? BLOCK_SIZE - r : 0
    end
    module_function :padding_size

    def head_read(io, magic_symbol)
      head_block = io.read(BLOCK_SIZE) or return
      if (head_block.size != BLOCK_SIZE) then
        raise BrokenError, 'short read'
      end

      _magic_symbol, body_len, fmt_version, head_cksum,
        body_hash_type, body_hash_len, body_hash_bucket = head_block.unpack(HEAD_FMT)

      head_block[HEAD_CKSUM_POS] = "\000\000"
      if (head_block.sum(HEAD_CKSUM_BITS) != head_cksum) then
        raise BrokenError, 'broken head block'
      end

      if (_magic_symbol != magic_symbol) then
        raise BrokenError, "unknown magic symbol: #{_magic_symbol}"
      end

      if (fmt_version != FMT_VERSION) then
        raise BrokenError, format('unknown format version: 0x%04F', fmt_version)
      end

      body_hash_bin = body_hash_bucket[0, body_hash_len]

      return body_len, body_hash_type, body_hash_bin
    end
    module_function :head_read

    def head_write(io, magic_symbol, body_len, body_hash_type, body_hash_bin)
      head_block = [
        magic_symbol,
        body_len,
        FMT_VERSION,
        0,
        body_hash_type,
        body_hash_bin.length,
        body_hash_bin
      ].pack(HEAD_FMT)

      head_cksum = head_block.sum(HEAD_CKSUM_BITS)
      head_block[HEAD_CKSUM_POS] = [ head_cksum ].pack(HEAD_CKSUM_FMT)

      bytes = io.write(head_block)
      if (bytes != head_block.size) then
        raise BrokenError, 'short write'
      end
      bytes
    end
    module_function :head_write

    def block_read(io, magic_symbol)
      body_len, body_hash_type, body_hash_bin = head_read(io, magic_symbol)
      unless (body_len) then
        return
      end

      body = io.read(body_len) or raise BrokenError, 'unexpected EOF'
      if (body.length != body_len) then
        raise BrokenError, 'short read'
      end

      unless (hash_proc = BODY_HASH_BIN[body_hash_type]) then
        raise BrokenError, "unknown body hash type: #{body_hash_type}"
      end

      if (hash_proc.call(body) != body_hash_bin) then
        raise BrokenError, 'body hash error'
      end

      padding_size = padding_size(body_len)
      padding = io.read(padding_size) or raise BrokenError, 'unexpected EOF'
      if (padding.size != padding_size) then
        raise BrokenError, 'short read'
      end

      body
    end
    module_function :block_read

    def block_write(io, magic_symbol, body, body_hash_type=:MD5)
      hash_proc = BODY_HASH[body_hash_type.to_sym] or "unknown body hash type: #{body_hash_type}"
      body_hash = hash_proc.call(body)
      head_write(io, magic_symbol, body.length, body_hash_type.to_s, body_hash)

      bytes = io.write(body)
      if (bytes != body.size) then
        raise BrokenError, 'short write'
      end

      padding_size = padding_size(body.length)
      bytes = io.write("\0" * padding_size)
      if (bytes != padding_size) then
        raise BrokenError, 'short write'
      end

      body.size + padding_size
    end
    module_function :block_write
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
