from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from helpers import FindMostLeastFrequent, PlotHistogram, ReadLog, FindCount, PlotHistogram, FindMostLeastFrequent, CountRequest 
from datetime import datetime
from helpers import ReadLog, FindCount, PlotHistogram, minMaxTrafficTime


class MainWindow(QMainWindow):
    # Initializes Window Geometry and Important Variables
    def __init__(self, parent=None, verbose = True):
        self.verbose = verbose
        self.PrintLog("Initializing Window")
        
        super(QMainWindow, self).__init__(parent)
        self.setWindowTitle("Proxy Log Analyser")
        self.resize(400, 225)
        
        self.filePath = ""
        self.logData = None
        self.styleSheet = ""
        with open("design.qss") as qss:
            self.styleSheet = qss.read()
        self.setStyleSheet(self.styleSheet)

        self.setupUi()

    def setupUi(self):
        self.PrintLog("Setting-Up User Interface")

        self.centralWidget = QWidget(self)
        self.setCentralWidget(self.centralWidget)

        self.centralLayout = QVBoxLayout()
        self.centralWidget.setLayout(self.centralLayout)

        # File Selection Buttons
        self.filePickerButton = QPushButton("&Choose file", self)
        self.filePickerButton.clicked.connect(self.FilePicker)

        # Selected File Display
        self.fileStatusLabel = QLabel(self, text="Status: No log selected")

        # Extra Feature Buttons
        self.plotTimeVsWebCountButton = QPushButton("Show &Time vs Number of Websited")
        self.plotTimeVsWebCountButton.clicked.connect(lambda : minMaxTrafficTime(self.dataFrame))

        self.plotWebsiteFrequencyButton = QPushButton("Frequency of Different Websites")
        self.plotWebsiteFrequencyButton.clicked.connect(lambda: PlotHistogram(self.dataFrame,"URL","Frequeny"))

        self.button3 = QPushButton("Button3")
        self.button4 = QPushButton("Button4")
        self.button5 = QPushButton("Button5")
        self.button6 = QPushButton("Button6")
        self.button7 = QPushButton("Button7")
        self.button8 = QPushButton("Button8")

        # Feature buttons arranged in a grid
        self.featureButtonsLayout = QGridLayout()
        self.featureButtonsLayout.addWidget(self.plotTimeVsWebCountButton, 0, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.plotWebsiteFrequencyButton, 0, 1, 1, 1)
        self.featureButtonsLayout.addWidget(self.button3, 1, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.button4, 1, 1, 1, 1)
        self.featureButtonsLayout.addWidget(self.button5, 2, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.button6, 2, 1, 1, 1)
        self.featureButtonsLayout.addWidget(self.button7, 3, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.button8, 3, 1, 1, 1)

        # Main Vertical Layout
        self.centralLayout.addWidget(self.filePickerButton)
        self.centralLayout.addWidget(self.fileStatusLabel)
        self.centralLayout.addLayout(self.featureButtonsLayout)

    # Choose File to perform Operations on
    def FilePicker(self):
        oldFilePath = self.filePath
        self.filePath = QFileDialog.getOpenFileName(self, "Open log File")

        # Reverts Changes if No file is Selected or Operation is Cancelled
        if self.filePath[0] == "":
            self.filePath = oldFilePath
            return
        
        fileDate = datetime.strptime(self.filePath.split('-')[-1], '%Y%m%d')
        
        self.fileStatusLabel.setText("Status: Loading Log of " + fileDate.strftime('%m/%d/%Y'))
        
        self.PrintLog("Loading File")
        self.logData = ReadLog(self.filePath[0])
        self.PrintLog("[LOG] File Loaded")

        self.fileStatusLabel.setText("Status: Log of " + fileDate.strftime('%m/%d/%Y') + "Loaded Successfully")

    def PrintLog(self, entry):
        if (self.verbose == True):
            print("[LOG] " + entry)
