# $Id$

require 'digest/sha2'

module Higgs
  class JournalLogger
    # for ident(1)
    CVS_ID = '$Id$'

    BLOCK_SIZE = 512

    HEAD_FMT = [
      'Z16',			# magic symbol
      'N',			# data length
      'x12',			# (reserved)
      'n',			# fmt version
      'x2',			# (reserved)
      'Z16',			# cksum type
      'n',			# cksum length
      'x10',			# (reserved),
      'a448'			# cksum data
    ].join('')

    MAGIC_SYMBOL = 'HIGGS_JLOG'
    FMT_VERSION  = 0x00_00
    CKSUM_TYPE   = 'SHA512'
    CKSUM_LENGTH = 64

    EOL = 0xAA.chr * BLOCK_SIZE

    def initialize(out)
      @out = out
    end

    def write(log)
      bin_log = Marshal.dump(log)
      head_block = [
	MAGIC_SYMBOL,
	bin_log.length,
	FMT_VERSION,
	CKSUM_TYPE,
	CKSUM_LENGTH,
	Digest::SHA512.digest(bin_log)
      ].pack(HEAD_FMT)
      @out.write(head_block)
      @out.write(bin_log)
      @out.write("\0" * JournalLogger.padding_size(bin_log.length))
      @out.write(EOL)
      @out.fsync
      self
    end

    def close
      @out.fsync
      @out.close
      @out = nil
      nil
    end

    class << self
      def padding_size(bytes)
	r = bytes % BLOCK_SIZE
	(r > 0) ? BLOCK_SIZE - r : 0
      end

      def open(path)
	f = File.open(path, File::WRONLY | File::CREAT | File::EXCL, 0660)
	f.binmode
	new(f)
      end

      def each_log(path)
	File.open(path, 'r') {|f|
	  f.binmode

	  while (head_block = f.read(BLOCK_SIZE))
	    magic_symbol, data_length, fmt_version, cksum_type, cksum_length, cksum_data = head_block.unpack(HEAD_FMT)

	    if (magic_symbol != MAGIC_SYMBOL) then
	      raise "unknown magic symbol: #{magic_symbol}"
	    end

	    if (fmt_version != FMT_VERSION) then
	      raise "unknown format version: #{format('0x%XX', fmt_version)}"
	    end

	    if (cksum_type != CKSUM_TYPE) then
	      raise "unknown checksum type: #{cksum_type}"
	    end

	    if (cksum_data.length < cksum_length) then
	      raise "invalid checksum data length: #{cksum_length}"
	    end
	    cksum_data = cksum_data[0, cksum_length]

	    bin_log = f.read(data_length) or raise 'unexpected EOF'
	    if (bin_log.length != data_length) then
	      raise 'found short data'
	    end
	    if (Digest::SHA512.digest(bin_log) != cksum_data) then
	      raise 'checksum error'
	    end

	    f.read(padding_size(data_length)) or raise 'unexpected EOF'

	    eol = f.read(BLOCK_SIZE) or raise 'unexpected EOF'
	    if (eol != EOL) then
	      raise 'broken EOL'
	    end

	    log = Marshal.load(bin_log)
	    yield(log)
	  end
	}
	nil
      end
    end
  end
end
