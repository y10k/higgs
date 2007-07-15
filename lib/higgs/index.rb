# storage index

require 'forwardable'
require 'higgs/block'

module Higgs
  # storage index
  class Index
    # for ident(1)
    CVS_ID = '$Id$'

    extend Forwardable
    include Block

    MAGIC_SYMBOL = 'HIGGS_INDEX'
    MAJOR_VERSION = 0
    MINOR_VERSION = 0

    def initialize
      @change_number = 0
      @eoa = 0
      @free_lists = {}
      @index = {}
    end

    attr_reader :change_number
    attr_accessor :eoa

    def succ!
      @change_number = @change_number.succ
      self
    end

    def free_fetch(size)
      @free_lists[size].shift if (@free_lists.key? size)
    end

    def free_fetch_at(pos, size)
      @free_lists[size].delete(pos) or raise "not found free region: (#{pos},#{size})"
    end

    def free_store(pos, size)
      @free_lists[size] = [] unless (@free_lists.key? size)
      @free_lists[size] << pos
      nil
    end

    def_delegator :@index, :[]
    def_delegator :@index, :[]=
    def_delegator :@index, :key?
    def_delegator :@index, :keys
    def_delegator :@index, :delete

    def each_key
      @index.each_key do |key|
        yield(key)
      end
      self
    end

    def to_h
      { :version => [ MAJOR_VERSION, MINOR_VERSION ],
        :change_number => @change_number,
        :eoa => @eoa,
        :free_lists => @free_lists,
        :index => @index
      }
    end

    def save(path)
      tmp_path = "#{path}.tmp.#{$$}"
      File.open(tmp_path, File::WRONLY | File::CREAT | File::TRUNC, 0660) {|f|
        f.binmode
        index_data = self.to_h
        block_write(f, MAGIC_SYMBOL, Marshal.dump(index_data))
        f.fsync
      }
      File.rename(tmp_path, path)
      self
    end

    def load(path)
      File.open(path, 'r') {|f|
        f.binmode
        index_data = Marshal.load(block_read(f, MAGIC_SYMBOL))
        if ((index_data[:version] <=> [ MAJOR_VERSION, MINOR_VERSION ]) > 0) then
          raise "unsupported version: #{index_data[:version].join('.')}"
        end
        @change_number = index_data[:change_number]
        @eoa = index_data[:eoa]
        @free_lists = index_data[:free_lists]
        @index = index_data[:index]
      }
      self
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
