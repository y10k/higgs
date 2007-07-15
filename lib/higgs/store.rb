# storage interface like pstore

require 'forwardable'
require 'higgs/storage'
require 'higgs/tman'

module Higgs
  # storage interface like pstore
  class Store
    # for ident(1)
    CVS_ID = '$Id$'

    extend Forwardable
    include Storage::Export

    DECODE = proc{|r| Marshal.load(r) }
    ENCODE = proc{|w| Marshal.dump(w) }

    def initialize(name, options={})
      options[:decode] = DECODE
      options[:encode] = ENCODE
      @storage = Storage.new(name, options)
      @tman = TransactionManager.new(@storage, options)
    end

    def_delegator :@tman, :read_only
    def_delegator :@tman, :transaction

    def self.open(*args)
      store = new(*args)
      begin
        r = yield(store)
      ensure
        store.shutdown
      end
      r
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
