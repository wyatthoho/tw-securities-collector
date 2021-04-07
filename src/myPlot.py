import matplotlib.pyplot as plt

def FindXtick(data, tickNum):
    inc = int( (len(data)-1) // (tickNum-1) )
    tick = [data[itr*inc] for itr in range(tickNum)]

    return tick


def FindYtick(data, tickNum):
    availableInc = [1, 2, 5, 10, 20, 50, 100, 200, 250, 500, 1000]
    requiredNum = [int(max(data)//inc + 2) for inc in availableInc]

    numDiff = [int(abs(num-tickNum)) for num in requiredNum]
    optIdx = numDiff.index(min(numDiff))

    inc = availableInc[optIdx]
    tick = [itr*inc for itr in range(requiredNum[optIdx])]

    return tick


def PlotData(figIdx, filePath, title, xData, yData):
    facecolor = (0.12, 0.12, 0.16)
    edgecolor = (0.92, 0.92, 0.92)
    linecolor = 'violet'

    xTick = FindXtick(xData, 8)
    yTick = FindYtick(yData, 6)

    fig = plt.figure(num=figIdx, figsize=(6,3), facecolor=facecolor, tight_layout=True)
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(xData, yData, color=linecolor, linewidth=0.9)

    ax.set_xticks(xTick)
    ax.set_xticklabels(xTick, rotation='vertical', color=edgecolor)
    ax.set_xlim((xTick[0], xTick[-1]))

    ax.set_yticks(yTick)
    ax.set_yticklabels(yTick, color=edgecolor)
    ax.set_ylim((yTick[0], yTick[-1]))

    ax.grid(color=edgecolor, linestyle='-', linewidth=0.3)
    ax.set_title('{}'.format(title), family='Arial', fontsize=14, color=edgecolor)
    ax.set_facecolor(facecolor)
    ax.spines['bottom'].set_color(edgecolor)
    ax.spines['top'].set_color(edgecolor)
    ax.spines['left'].set_color(edgecolor)
    ax.spines['right'].set_color(edgecolor)
    plt.savefig(filePath)