#!/usr/local/bin/ruby

$: << File.join(File.dirname($0), '..', 'lib')

require 'higgs/jlog'
require 'pp'

# for ident(1)
CVS_ID = '$Id$'

Higgs::JournalLogger.each_log(ARGV[0]) {|log|
  pp log
}

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
