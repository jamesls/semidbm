#!/usr/bin/env python

# Ok so I know this is basically a bs metric, but it's still fun to see what the numbers
# are.  But be warned, don't take these numbers too seriously.
import tempfile
import time
import semidbm
import optparse


def main():
    parser = optparse.OptionParser()
    parser.add_option('-n', '--num-transactions', default=1000000, type=int)
    parser.add_option('-r', '--repeat', default=10, type=int)
    parser.add_option('-g', '--groups', default=10000, type=int)
    opts, args = parser.parse_args()

    f = tempfile.NamedTemporaryFile()
    db = semidbm.open(f.name, 'c')

    num_transactions = opts.num_transactions
    groups_of = opts.groups
    repeat = opts.repeat

    start = time.time()
    for i in xrange(num_transactions):
        db[str(i)] = str(i)
    end = time.time()
    print "Write ",
    print "Total: %.5f, tps: %.2f" % (end - start,
                                      float(num_transactions) / (end - start))
    db = semidbm.open(f.name, 'r')
    start = time.time()
    for i in xrange(num_transactions):
        db[str(i)]
    end = time.time()

    print "Read ",
    print "Total: %.5f, tps: %.2f" % (end - start,
                                      float(num_transactions) / (end - start))


    count = 0
    start = time.time()
    for i in xrange(0, num_transactions, groups_of):
        for j in xrange(groups_of):
            for k in xrange(repeat):
                count += 1
                db[str(i + j)]
    end = time.time()
    print "Read (grouped)",
    print "count:", count
    print "Total: %.5f, tps: %.2f" % (end - start,
                                      float(count) / (end - start))


if __name__ == '__main__':
    main()
