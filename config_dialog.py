import os
import json
from PyQt6.QtWidgets import (QDialog, QLineEdit, QPushButton, QFormLayout, QMessageBox)


class ConfigDialog(QDialog):
    def __init__(self, config_file, parent=None):
        super().__init__(parent)
        self.config_file = config_file
        self.config = self.load_config()
        self.init_ui()

    def load_config(self):
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def save_config(self):
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(self.config, f, indent=4)

    def init_ui(self):
        self.setWindowTitle("Configuration")
        self.setGeometry(100, 100, 400, 200)

        layout = QFormLayout()

        self.api_key_edit = QLineEdit(self.config.get('apikey', ''))
        self.project_code_edit = QLineEdit(self.config.get('project_code', ''))
        self.project_code_edit.setReadOnly(True)

        self.license_server_edit = QLineEdit(self.config.get('license_server', ''))
        self.license_server_edit.setReadOnly(True)

        self.api_base_url_edit = QLineEdit(self.config.get('apibaseurl', ''))
        self.api_base_url_edit.setReadOnly(True)

        self.software_edit = QLineEdit(self.config.get('software', ''))
        self.software_edit.setReadOnly(True)


        layout.addRow("API Key:", self.api_key_edit)
        layout.addRow("Project Code:", self.project_code_edit)
        layout.addRow("License Server:", self.license_server_edit)
        layout.addRow("API Base URL:", self.api_base_url_edit)
        layout.addRow("Software:", self.software_edit)

        save_button = QPushButton("Save")
        save_button.clicked.connect(self.save_settings)
        layout.addWidget(save_button)

        self.setLayout(layout)

    def save_settings(self):
        self.config['apikey'] = self.api_key_edit.text()

        self.save_config()
        QMessageBox.information(self, "Configuration", "Settings saved successfully.")
        self.accept()
