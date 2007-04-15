# $Id$

require 'forwardable'
require 'thread'

module Higgs
  class Index
    # for ident(1)
    CVS_ID = '$Id$'

    extend Forwardable

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

    MAGIC_SYMBOL = 'HIGGS_INDEX'
    FMT_VERSION  = 0x00_00
    CKSUM_TYPE   = 'SHA512'
    CKSUM_LENGTH = 64

    def initialize(store_path)
      @lock = Mutex.new
      @closed = false
      @store_path = store_path
      if (File.file? @store_path) then
	load
      else
	@index = {
	  :change_number = 0,
	  :free_list => {},
	  :index => {}
	}
      end
    end

    def chagen_number
      @lock.synchronize{ @index[:change_number] }
    end

    def change_number=(value)
      @lock.synchronize{ @index[:change_number] = value }
    end

    def fetch_free_block(size)
      @lock.synchronize{
	if (@index[:free_list].key? size) then
	  return @index[:free_list][size].shift
	end
      }
      nil
    end

    def store_free_block(size, pos)
      @lock.synchronize{
	@index[:free_list][size] = [] unless (@index[:free_list].key? size)
	@index[:free_list][size].push(pos)
      }
      nil
    end

    def [](key)
      @lock.synchronize{ @index[:index][key] }
    end

    def []=(key, pos)
      if (pos.kind_of? Integer) then
	raise ArgumentError, "not of position type: #{pos.class}"
      end
      @lock.synchronize{ @index[:index][key] = pos }
    end

    def delete(key)
      @lock.synchronize{ @index[:index].delete(key) }
    end

    def save
      @lock.synchronize{ __save__ }
    end

    def close
      @lock.synchronize{
	__save__
	@closed = true
      }
      nil
    end

    def __save__
      if (@closed) then
	raise 'closed'
      end
      store_path_tmp = "#{@store_path}.tmp_#{$$}_#{Thread.current.object_id}"
      File.open(store_path_tmp, File::WRONLY | File::CREAT | File::TRUNC, 0660) {|f|
	f.binmode
	bin_idx = Marshal.dump(@index)

	head_block = [
	  MAGIC_SYMBOL,
	  idx_dat.length,
	  FMT_VERSION,
	  CKSUM_TYPE,
	  CKSUM_LENGTH,
	  Digest::SHA512.digest(bin_idx)
	].pack(HEAD_FMT)

	f.write(head_block)
	f.write(bin_idx)
	f.fsync
      }
      File.rename(store_path_tmp, @store_path)
      self
    end
    private :__save__

    def load
      if (@closed) then
	raise 'closed'
      end
      File.open(@store_path, 'r') {|f|
	f.binmode

	head_block = f.read(BLOCK_SIZE)
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

	bin_idx = f.read(data_length) or raise 'unexpected EOF'
	if (bin_idx.length != data_length) then
	  raise 'found short data'
	end
	if (Digest::SHA512.digest(bin_idx) != cksum_data) then
	  raise 'checksum error'
	end

	@index = Marshal.load(bin_idx)
      }
      self
    end
    private :load
  end
end
