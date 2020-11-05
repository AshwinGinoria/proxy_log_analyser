from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *



class MainWindow(QMainWindow):
    # Initializes Window Geometry and Important Variables
    def __init__(self, parent=None):
        super(QMainWindow, self).__init__(parent)
        self.setObjectName("MainWindow")
        self.setWindowTitle("Proxy Log Analyser")
        self.FilePath = ""
        self.resize(600, 400)
        self.setupUi()

    def setupUi(self):
        self.centralWidget = QWidget(self)
        self.setCentralWidget(self.centralWidget)

        self.centralLayout = QVBoxLayout()
        self.centralWidget.setLayout(self.centralLayout)

        # File Selection Buttons
        self.filePickerButton = QPushButton("&Choose file", self)
        self.filePickerButton.clicked.connect(self.FilePicker)

        self.refreshFileButton = QPushButton("&Refresh File", self)
        self.refreshFileButton.clicked.connect(self.RefreshFile)

        # Buttons Arranged Horizontally
        self.buttonLayout = QHBoxLayout()
        self.buttonLayout.addWidget(self.filePickerButton)
        self.buttonLayout.addWidget(self.refreshFileButton)

        # File Content Display
        self.fileTextViewBox = QTextEdit()
        self.fileTextViewBox.setReadOnly(True)

        # Extra Feature Buttons
        self.plotTimeVsWebCountButton = QPushButton("Show &Time vs Number of Websited")
        #self.plotTimeVsWebCount.clicked.connect(self.func)

        self.plotTimeVsBlockedWebCountButton = QPushButton("Show Time vs &Blocked Websites count")
        #self.plotTimeVsBlockedWebCountButton.clicked.connect(self.func)

        # Feature buttons arranged in a grid
        self.featureButtonsLayout = QGridLayout()
        self.featureButtonsLayout.addWidget(self.plotTimeVsWebCountButton,0,0,1,1)
        self.featureButtonsLayout.addWidget(self.plotTimeVsBlockedWebCountButton,0,1,1,1)

        # Main Vertical Layout
        self.centralLayout.addLayout(self.buttonLayout)
        self.centralLayout.addWidget(self.fileTextViewBox)
        self.centralLayout.addLayout(self.featureButtonsLayout)

    # Refreshes Currently open file for any outside changes made to it
    def RefreshFile(self):
        with open(self.FilePath[0], "r") as File:
            text = File.read()
            self.FileTextViewBox.setText(text)

    # Choose File to perform Operations on
    def FilePicker(self):
        OldFilePath = self.FilePath
        self.FilePath = QFileDialog.getOpenFileName(self, "Open log File")

        # Reverts Changes if No file is Selected or Operation is Cancelled
        if self.FilePath[0] == "":
            self.FilePath = OldFilePath
            return

        # Updates FileTextViewBox with new text
        with open(self.FilePath[0], "r") as File:
            text = File.read()
            self.fileTextViewBox.setText(text)

    