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


class inputdialogdemo(QDialog):
    def __init__(self, parent=None):
        super(inputdialogdemo, self).__init__(parent)

        self.layout = QFormLayout(self)
        self.resize(600, 900)
        self.DifferentWebsites = QTableWidget()
        self.DifferentWebsites.setRowCount(5)
        self.DifferentWebsites.setColumnCount(2)
        self.DifferentWebsites.setColumnWidth(0, 400)
        self.DifferentWebsites.setEditTriggers(QTableWidget.NoEditTriggers)
        self.DifferentWebsites.setHidden(False)
        StartTimeLabel = QLabel("Start time: ")
        EndTimeLabel = QLabel("End time: ")
        self.start_time = QLineEdit(self)
        self.start_time.setPlaceholderText("Time format: dd/mm/yy hh:mm:ss")
        self.end_time = QLineEdit(self)
        self.end_time.setPlaceholderText("Time format: dd/mm/yy hh:mm:ss")
        self.button = QPushButton("submit")

        self.button.clicked.connect(
            lambda: self.DisplayDict(
                helpers.GetNumberOfUniqueWebsites(
                    self.start_time.text(), self.end_time.text()
                )
            )
        )

        self.layout.addWidget(StartTimeLabel)
        self.layout.addWidget(self.start_time)
        self.layout.addWidget(EndTimeLabel)
        self.layout.addWidget(self.end_time)
        self.layout.addWidget(self.button)
        self.layout.addWidget(self.DifferentWebsites)

    def DisplayDict(self, data, title="DLG"):
        i = 0
        self.DifferentWebsites.setHorizontalHeaderLabels(["Field", "Value"])
        for key, val in data.items():
            self.DifferentWebsites.setItem(i, 0, QTableWidgetItem(key))
            self.DifferentWebsites.setItem(i, 1, QTableWidgetItem(str(val)))
            i += 1
        self.DifferentWebsites.setHidden(False)


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
            lambda: self.DisplayDict(helpers.GetTopClients(), "Top 10 Clients")
        )

        self.button4 = QPushButton("Analysis for given period")
        self.button4.clicked.connect(self.on_button4_clicked)

        self.button5 = QPushButton("Button5")
        self.button6 = QPushButton("Button6")
        self.button7 = QPushButton("Button7")
        self.button8 = QPushButton("Button8")

        # Feature buttons arranged in a grid
        self.featureButtonsLayout = QGridLayout()
        self.featureButtonsLayout.addWidget(self.plotTimeVsWebCountButton, 0, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.plotWebsiteFrequencyButton, 0, 1, 1, 1)
        self.featureButtonsLayout.addWidget(self.topClientsButton, 1, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.button4, 1, 1, 1, 1)
        self.featureButtonsLayout.addWidget(self.button5, 2, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.button6, 2, 1, 1, 1)
        self.featureButtonsLayout.addWidget(self.button7, 3, 0, 1, 1)
        self.featureButtonsLayout.addWidget(self.button8, 3, 1, 1, 1)

        # Main Vertical Layout
        self.centralLayout.addWidget(self.filePickerButton)
        self.centralLayout.addWidget(self.fileStatusLabel)
        self.centralLayout.addWidget(self.displayTable)
        self.centralLayout.addWidget(self.plotWidget)
        self.centralLayout.addLayout(self.featureButtonsLayout)

        logger.debug("UI-Setup complete!!")

    def on_button4_clicked(self):
        self.dialog = inputdialogdemo(self)
        self.dialog.exec_()

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
        for val in values:
            self.displayTable.setItem(i, 0, QTableWidgetItem(str(val[0])))
            self.displayTable.setItem(i, 1, QTableWidgetItem(str(val[1])))
            i += 1

        self.displayTable.setHidden(False)
        self.plotWidget.setHidden(True)
