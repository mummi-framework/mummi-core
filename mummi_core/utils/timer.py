# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import timeit


# ------------------------------------------------------------------------------
# timing utilities
# ------------------------------------------------------------------------------
class Timer(object):

    def __init__(self):
        self.start()

    def start(self):
        self.stime = timeit.default_timer()
        self.etime = -1

    def end(self):
        self.etime = timeit.default_timer()

    def elapsed(self):

        etime = self.etime
        if etime < 0: etime = timeit.default_timer()
        return etime - self.stime

    def __str__(self):

        tseconds = self.elapsed()

        s = ' [[ elapsed time: '
        if tseconds < 0.000001:
            s = s + ("%.3f micro-sec." % (tseconds*1000000))

        elif tseconds <  0.001:
            s = s + ("%.3f milli-sec." % (tseconds*1000))

        elif tseconds < 60.0:
            s = s + ("%.3f sec." % (tseconds))

        else:
            m = int(tseconds/60.0)
            s = s + ("%d min. %.3f sec." % (m, (tseconds - 60*m)))

        s = s + ' ]]'
        return s

    def __repr__(self):
        return self.__str__()

# ------------------------------------------------------------------------------
