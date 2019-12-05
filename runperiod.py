def runperiod(run):
    """ subroutine to determine run period """
    if (run < 5220):
        period = "Run0"
    elif(run < 8000):
        period ="Run1"
    elif (run < 9277):
        period = "Run2a"
    elif (run < 9859):
        period = "Run2b"
    elif (run < 10234):
        period = "Run2c"
    elif (run < 10663):
        period = "Run2t"
    elif (run < 12890):
        period = "Run3a"
    else:
        period = "Run3b"
    return period

