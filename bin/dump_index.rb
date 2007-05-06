#!/usr/local/bin/ruby

$: << File.join(File.dirname($0), '..', 'lib')

require 'higgs/index'
require 'pp'

# for ident(1)
CVS_ID = '$Id$'

index = Higgs::Index.new
index.load(ARGV[0])
pp index

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
