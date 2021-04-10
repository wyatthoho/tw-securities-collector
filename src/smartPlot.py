import matplotlib.pyplot as plt


class BilinearFig:
    def __init__(self, figIdx):
        self.figIdx = figIdx
        self.figsize = (6, 3)
        self.facecolor = (0.12, 0.12, 0.16)
        self.edgecolor = (0.92, 0.92, 0.92)
        self.linecolor = 'violet'
        self.linewidth = 0.9
        self.xTickNum = 8
        self.yTickNum = 6

        self.fig = plt.figure(num=figIdx, figsize=self.figsize, facecolor=self.facecolor, tight_layout=True)
        self.ax = self.fig.add_subplot(1, 1, 1)


    def findXtick(self, data):
        inc = int( (len(data)-1) // (self.xTickNum-1) )
        tick = [data[itr*inc] for itr in range(self.xTickNum)]

        return tick


    def findYtick(self, data):
        availableInc = [1, 2, 5, 10, 20, 50, 100, 200, 250, 500, 1000]
        requiredNum = [int(max(data)//inc + 2) for inc in availableInc]

        numDiff = [int(abs(num-self.yTickNum)) for num in requiredNum]
        optIdx = numDiff.index(min(numDiff))

        inc = availableInc[optIdx]
        tick = [itr*inc for itr in range(requiredNum[optIdx])]

        return tick
    

    def plotData(self, title, xData, yData):
        self.ax.plot(xData, yData, color=self.linecolor, linewidth=self.linewidth)

        xTick = self.findXtick(xData)
        yTick = self.findYtick(yData)

        self.ax.set_xticks(xTick)
        self.ax.set_xticklabels(xTick, rotation='vertical', color=self.edgecolor)
        self.ax.set_xlim((xTick[0], xTick[-1]))

        self.ax.set_yticks(yTick)
        self.ax.set_yticklabels(yTick, color=self.edgecolor)
        self.ax.set_ylim((yTick[0], yTick[-1]))

        self.ax.grid(color=self.edgecolor, linestyle='-', linewidth=0.3)
        self.ax.set_title('{}'.format(title), family='Arial', fontsize=14, color=self.edgecolor)
        self.ax.set_facecolor(self.facecolor)
        self.ax.spines['bottom'].set_color(self.edgecolor)
        self.ax.spines['top'].set_color(self.edgecolor)
        self.ax.spines['left'].set_color(self.edgecolor)
        self.ax.spines['right'].set_color(self.edgecolor)

    def saveFig(self, filePath):
        plt.savefig(filePath)
        plt.close(self.figIdx)

