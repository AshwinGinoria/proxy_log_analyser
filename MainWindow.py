from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import matplotlib.pyplot as plt
from datetime import datetime
from helpers import Helpers
import logging

helpers = Helpers()
logging.basicConfig()
logger = logging.getLogger("MainWindow")
logger.setLevel(logging.DEBUG)


class MainWindow(QMainWindow):

    # Initializes Window Geometry and Important Variables
    def __init__(self, parent=None):
        logger.info("Application Started")
        logger.debug("Initilizing Instance of MainWindow")

        super(QMainWindow, self).__init__(parent)
        self.setWindowTitle("Proxy Log Analyser")
        self.resize(800, 700)

        self.fileNames = ""
        self.styleSheet = ""
        with open("design.qss") as qss:
            self.styleSheet = qss.read()
        self.setStyleSheet(self.styleSheet)

        self.SetupUi()

        logger.info("Initialization Complete")

    def SetupUi(self):
        logger.info("Setting-Up User Interface")

        self.centralWidget = QWidget(self)
        self.setCentralWidget(self.centralWidget)

        self.centralLayout = QVBoxLayout()
        self.centralWidget.setLayout(self.centralLayout)

        # File Selection Buttons
        self.filePickerButton = QPushButton("&Choose file", self)
        self.filePickerButton.clicked.connect(self.FilePicker)

        # Selected File Display
        self.fileStatusLabel = QLabel(self, text="Status: No log selected")

        self.dlgText = QTextEdit(self.centralWidget)
        # self.dlgText.setHidden(True)

        # Creating canvas for displaying plots on main window
        self.plotWidget = QWidget(self.centralWidget)
        self.fig, self.ax = plt.subplots()
        self.figWidget = FigureCanvas(self.fig)
        self.figToolbar = NavigationToolbar(self.figWidget, self.plotWidget)

        self.plotLayout = QVBoxLayout(self.plotWidget)
        self.plotLayout.addWidget(self.figToolbar)
        self.plotLayout.addWidget(self.figWidget)
        self.plotLayout.setContentsMargins(15, 15, 15, 15)

        self.plotWidget.setLayout(self.plotLayout)
        self.plotWidget.setHidden(True)

        # Extra Feature Buttons
        self.plotTimeVsWebCountButton = QPushButton("Show &Time vs Number of Websites")
        self.plotTimeVsWebCountButton.clicked.connect(
            lambda: self.PlotOnCanvas(helpers.PlotAcceptedDeniedCount)
        )

        self.plotWebsiteFrequencyButton = QPushButton("Frequency of Different Websites")
        self.plotWebsiteFrequencyButton.clicked.connect(
            lambda: self.PlotOnCanvas(helpers.CountWebsite)
        )

        self.button3 = QPushButton("Top 10 Clients")
        self.button3.clicked.connect(
            lambda: self.DisplayDict(helpers.GetTopClients(), "Top 10 Clients")
        )
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
        self.centralLayout.addWidget(self.dlgText)
        self.centralLayout.addWidget(self.plotWidget)
        self.centralLayout.addLayout(self.featureButtonsLayout)

        logger.debug("UI-Setup complete!!")

    # Choose File to perform Operations on
    def FilePicker(self):
        logger.info("Opening File Picker")

        oldFileNames = self.fileNames
        self.fileNames = QFileDialog.getOpenFileNames(self, "Open log Files")

        # Reverts Changes if No file is Selected or Operation is Cancelled
        if self.fileNames[0] == []:
            logger.debug("No file path provided")
            self.fileNames = oldFileNames
            return

        helpers.ReadLog(listFiles=self.fileNames[0])

        self.fileStatusLabel.setText(
            "Status: " + str(len(self.fileNames[0])) + " Loaded Successfully"
        )

    def PlotOnCanvas(self, plottingFunc):
        logger.info("Plotting Data")
        self.ax.cla()

        self.ax = plottingFunc(self.ax)

        plt.tight_layout()

        self.figWidget.draw()

        logger.debug("Displaying Plot")
        self.dlgText.setHidden(True)
        self.plotWidget.setHidden(False)

    def DisplayDict(self, data, title="DLG"):
        logger.info("Parsing Recieved Data")
        displayText = ""

        for key, val in data.items():
            displayText += key + ": " + str(val) + "\n"

        self.dlgText.setReadOnly(True)
        self.dlgText.setText(displayText)

        self.plotWidget.setHidden(True)
        self.dlgText.setHidden(False)
