def TransRocToGregorian(rocDate, splitter='/'):
    '''
    Transform the ROC calendar to Gregorian calendar

    rocDate should be in the form as: yyy/mm/dd
    the gregorian would be: yyyy/mm/dd

    where yyyy = yyy + 1911

    '''
    rocYear = rocDate.split(splitter)[0]
    remaining = rocDate[len(rocYear):]
    gregorian = str(int(rocYear) + 1911) + remaining

    return gregorian

