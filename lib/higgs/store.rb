# storage interface like pstore
#
# Author:: $Author$
# Date:: $Date$
# Revision:: $Revision$
#

require 'higgs/storage'
require 'higgs/tman'

module Higgs
  # storage interface like pstore
  #
  # ex: sample/count.rb
  #   :include: sample/count.rb
  #
  class Store
    # for ident(1)
    CVS_ID = '$Id$'

    include Storage::Export
    include TransactionManager::Export

    DECODE = proc{|r| Marshal.load(r) }
    ENCODE = proc{|w| Marshal.dump(w) }

    # <tt>name</tt> is a storage name and see Higgs::Storage for detail.
    # see Higgs::Storage and Higgs::TransactionManager for <tt>options</tt>.
    def initialize(name, options={})
      options[:decode] = DECODE
      options[:encode] = ENCODE
      @storage = Storage.new(name, options)
      @tman = TransactionManager.new(@storage, options)
    end

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
