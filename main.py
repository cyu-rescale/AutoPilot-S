import sys
from PyQt6.QtWidgets import QApplication
from gui_program import GUIProgram

if __name__ == '__main__':
    app = QApplication(sys.argv)
    gui = GUIProgram()
    gui.show()
    sys.exit(app.exec())

"""
History
    v1: First release of AutoPilot-S
"""