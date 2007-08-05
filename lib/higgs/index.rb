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
    MINOR_VERSION = 1

    def initialize
      @change_number = 0
      @eoa = 0
      @free_lists = {}
      @index = {}
      @identities = {}
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

    def_delegator :@index, :key?
    def_delegator :@index, :keys

    def identity(key)
      i = @index[key] and i[0]
    end

    def [](key)
      i = @index[key] and i[1]
    end

    def self.create_id(key, identities)
      id = key.to_s
      if (identities.key? id) then
        id += '.a'
        id.succ! while (identities.key? id)
      end
      id
    end

    def []=(key, value)
      delete(key)
      id = Index.create_id(key, @identities)
      @identities[id] = key
      @index[key] = [ id, value ]
      value
    end

    def delete(key)
      id, value = @index.delete(key)
      @identities.delete(id) if id
      value
    end

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
        :index => @index,
        :identities => @identities
      }
    end

    def migration_0_0_to_0_1(index_data)
      if ((index_data[:version] <=> [ 0, 0 ]) > 0) then
        return
      end
      if ((index_data[:version] <=> [ 0, 0 ]) < 0) then
        raise "unexpected index format version: #{index_data[:version].join('.')}"
      end

      index = index_data[:index]
      identities = index_data[:identities] = {}
      for key in index.keys
        id = Index.create_id(key, identities)
        identities[id] = key
        value = index[key]
        index[key] = [ id, value ]
      end
      index_data[:version] = [ 0, 1 ]

      index_data
    end
    private :migration_0_0_to_0_1

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
        migration_0_0_to_0_1(index_data)
        if ((index_data[:version] <=> [ MAJOR_VERSION, MINOR_VERSION ]) > 0) then
          raise "unsupported version: #{index_data[:version].join('.')}"
        end
        @change_number = index_data[:change_number]
        @eoa = index_data[:eoa]
        @free_lists = index_data[:free_lists]
        @index = index_data[:index]
        @identities = index_data[:identities]
      }
      self
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
