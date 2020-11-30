import logging
from datetime import datetime

import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *

from helpers import Helpers

helpers = Helpers()
logging.basicConfig()
logger = logging.getLogger("MainWindow")
logger.setLevel(logging.DEBUG)

class MainWindow(QMainWindow):

    # Initializes Window Geometry and Important Variables
    def __init__(self, parent=None):
        logger.info("Application Started")
        logger.debug("Initilizing Instance of MainWindow")

        super(MainWindow, self).__init__(parent)
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

        # Creating table to display Number of requests for top users
        self.displayTable = QTableWidget()
        # self.displayTable.setHidden(True)

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

        self.topClientsButton = QPushButton("Top 10 Clients")
        self.topClientsButton.clicked.connect(
            lambda: self.DisplayDict(helpers.GetTopClients(), "Top Clients")
        )

        self.timeIntervalDataButton = QPushButton("Analysis for given Time interval")
        self.timeIntervalDataButton.clicked.connect(lambda: timeIntervalDlg(self).exec_())

        self.urlCategories = QPushButton("Get URL Categories")
        self.urlCategories.clicked.connect(
            lambda: self.DisplayDict(helpers.GetURICategories(), "URI Categories")
        )

        self.httpButton = QPushButton("Plot HTTP Response Occurence")
        self.httpButton.clicked.connect(
            lambda: self.PlotOnCanvas(helpers.PlotHttpCode)
        )

        # Feature buttons arranged in a grid
        self.featureButtonsLayout = QGridLayout()
        self.featureButtonsLayout.addWidget(self.plotTimeVsWebCountButton, 0, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.plotWebsiteFrequencyButton, 0, 1, 1, 1)
        self.featureButtonsLayout.addWidget(self.topClientsButton, 1, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.timeIntervalDataButton, 1, 1, 1, 1)
        self.featureButtonsLayout.addWidget(self.urlCategories, 2, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.httpButton, 2, 1, 1, 1)

        # Main Vertical Layout
        self.centralLayout.addWidget(self.filePickerButton)
        self.centralLayout.addWidget(self.fileStatusLabel)
        self.centralLayout.addWidget(self.displayTable)
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

        self.DisplayDict(helpers.ReadLog(listFiles=self.fileNames[0]))

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
        self.displayTable.setHidden(True)
        self.plotWidget.setHidden(False)

    def DisplayDict(self, data, title="DLG"):
        labels = data["labels"]
        values = data["values"]

        self.displayTable.setRowCount(len(values))
        self.displayTable.setColumnCount(len(labels))
        self.displayTable.setEditTriggers(QTableWidget.NoEditTriggers)
        self.displayTable.setHorizontalHeaderLabels(labels)
        self.displayTable.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeToContents
        )

        i = 0
        for row in values:
            j = 0
            for val in row:
                self.displayTable.setItem(i, j, QTableWidgetItem(str(val)))
                j += 1
            i += 1

        self.displayTable.setHidden(False)
        self.plotWidget.setHidden(True)

class timeIntervalDlg(QDialog):
    def __init__(self, parent=None):
        super().__init__()
        self.parent = parent
        self.styleSheet = ""
        with open("design.qss") as qss:
            self.styleSheet = qss.read()
        self.setStyleSheet(self.styleSheet)
        self.layout = QFormLayout(self)
        self.resize(300, 200)
        StartTimeLabel = QLabel("Start time: ")
        EndTimeLabel = QLabel("End time: ")
        self.start_time = QLineEdit(self)
        self.start_time.setPlaceholderText("Time format: dd/mm/yy hh:mm:ss")
        self.end_time = QLineEdit(self)
        self.end_time.setPlaceholderText("Time format: dd/mm/yy hh:mm:ss")
        self.button = QPushButton("Submit")

        self.button.clicked.connect(self.onClick)
        
        self.layout.addWidget(StartTimeLabel)
        self.layout.addWidget(self.start_time)
        self.layout.addWidget(EndTimeLabel)
        self.layout.addWidget(self.end_time)
        self.layout.addWidget(self.button)
    def onClick(self):
        self.parent.DisplayDict(
                helpers.GetNumberOfUniqueWebsites(
                    self.start_time.text(), self.end_time.text()
                )
            )
        self.close()
