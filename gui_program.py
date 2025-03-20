import csv
import json
import sys
from os import path, walk
from threading import Lock
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QLabel, QTextEdit,
                            QComboBox, QRadioButton, QPushButton, QListWidget, QMessageBox,
                            QAbstractItemView, QMainWindow, QFileDialog, QTabWidget)
from PyQt6.QtCore import QThreadPool, pyqtSignal, QObject
from PyQt6.QtGui import QIcon, QAction
from config_dialog import ConfigDialog
from worker import SubmitWorker
from api import RescaleAPI


CONFIG_FILE = "config_miscellaneous.json"
CORETYPE_CONFIGURATION = {
    "hematite": 64,
    "natrolite": 96
}
DEFAULT_NODE_COUNT = 3

class LogStream(QObject):
    new_log = pyqtSignal(str)

    def write(self, message):
        self.new_log.emit(str(message))

    def flush(self):
        pass

class GUIProgram(QMainWindow):
    job_status_updated = pyqtSignal()
    log_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.config = self.load_config()
        self.api = RescaleAPI(self.config['apibaseurl'], self.config['apikey'])
        self.threadpool = QThreadPool()
        self.job_threadpool = QThreadPool()
        self.job_threadpool.setMaxThreadCount(1)
        self.node_count = DEFAULT_NODE_COUNT

        self.init_ui()
        self.update_node_core_labels()
        self.log_signal.connect(self.update_log)

    # UI 초기화
    def init_ui(self):
        self.setWindowTitle("Rescale AutoPilot-S for HKMC R&D Aerodynamics Development Team")
        self.setWindowIcon(QIcon("Rescale.ico"))
        self.setGeometry(100, 100, 1200, 800)

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)

        self.main_layout = QVBoxLayout(self.central_widget)

        self.tab_widget = QTabWidget()
        self.tab_main = QWidget()
        self.tab_log = QWidget()
        self.tab_widget.addTab(self.tab_main, "Main")
        self.tab_widget.addTab(self.tab_log, "Log")
        
        self.main_layout.addWidget(self.tab_widget)

        # Main Tab 설정
        self.main_layout_tab = QHBoxLayout(self.tab_main)
        self.left_layout = QVBoxLayout()
        self.right_layout = QVBoxLayout()
        
        self.setup_left_layout()
        self.setup_right_layout()
        
        self.main_layout_tab.addLayout(self.left_layout)
        self.main_layout_tab.addLayout(self.right_layout)

        # Log Tab 설정
        self.log_text_edit = QTextEdit()
        self.log_text_edit.setReadOnly(True)
        log_layout = QVBoxLayout(self.tab_log)
        log_layout.addWidget(self.log_text_edit)
        
        # Redirect stdout to log text edit
        self.log_stream = LogStream()
        self.log_stream.new_log.connect(self.update_log)
        sys.stdout = self.log_stream
        sys.stderr = self.log_stream
        
        self.create_menu()


    def update_log(self, message):
        lines = self.log_text_edit.toPlainText().split('\n')

        if len(lines) > 0 and ("Downloading" in lines[-1] or "Transferring" in lines[-1]):
            lines[-1] = message
            self.log_text_edit.setPlainText('\n'.join(lines))
        else:
            self.log_text_edit.append(message)

#        self.log_text_edit.ensureCursorVisible()
        scrollbar = self.log_text_edit.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    # 왼쪽 레이아웃 설정
    def setup_left_layout(self):
        # 작업 설정 그룹 생성
        self.job_settings_group = QGroupBox("작업 설정")
        job_settings_layout = QVBoxLayout()

        # 작업 유형 선택 (Radio Group을 작업 설정 그룹에 포함)
        self.job_type_group = self.create_radio_group("작업 유형:", ["통합 작업"])        

        # 코어타입 유형 선택 (Radio Group을 작업 설정 그룹에 포함)
        self.coretype_group = self.create_radio_group("코어타입:", ["hematite", "natrolite"])
        self.coretype_group.findChildren(QRadioButton)[0].setChecked(True)
        job_settings_layout.addWidget(self.coretype_group)

        # 코어타입 선택에 대한 콜백 함수 추가
        for radio_button in self.coretype_group.findChildren(QRadioButton):
            radio_button.clicked.connect(self.update_node_core_labels)

        # Version 설정
        version_layout = QHBoxLayout()
        version_label = QLabel("STAR-CCM+ 버전:")
        self.version_combo = QComboBox()
        self.version_combo.addItems(["15.02.009 (Mixed Precision + AEROTv231207)",
                                    "15.02.009 (Double Precision + AEROTv231207)"])
        version_layout.addWidget(version_label)
        version_layout.addWidget(self.version_combo)
        job_settings_layout.addLayout(version_layout)

        # Node Control 설정
        self.node_label, self.core_label = self.create_node_control()
        job_settings_layout.addLayout(self.create_node_layout())

        # Walltime 설정
        walltime_layout = QHBoxLayout()
        walltime_label = QLabel("최대 실행 시간:")
        self.walltime_combo = QComboBox()
        self.walltime_combo.addItems(["24", "72", "120", "168", "240"])
        self.walltime_combo.setCurrentText("72")
        walltime_layout.addWidget(walltime_label)
        walltime_layout.addWidget(self.walltime_combo)
        job_settings_layout.addLayout(walltime_layout)

        self.job_settings_group.setLayout(job_settings_layout)
        self.left_layout.addWidget(self.job_settings_group)

        # 입력 파일 및 파일 목록 그룹 생성
        self.file_group = QGroupBox("입력 파일 및 목록")
        file_group_layout = QVBoxLayout()
        
        # 입력 파일 경로 설정
        self.input_label = QLabel("입력 파일 경로:")
        self.input_directory_label = QLabel("경로를 선택하지 않았습니다.")
        self.input_directory_label.setStyleSheet("background-color: lightgrey; border-radius: 5px; padding: 5px;")
        self.input_directory_button = QPushButton("경로를 선택하세요.")
        self.input_directory_button.clicked.connect(self.open_input_directory_dialog)
        
        file_group_layout.addWidget(self.input_label)
        file_group_layout.addWidget(self.input_directory_label)
        file_group_layout.addWidget(self.input_directory_button)
        
        # 파일 목록을 표시할 리스트 위젯(디렉토리 .sim 및 .java를 각각 표기)
        self.dir_list_widget = QListWidget()
        self.dir_list_widget.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        self.dir_list_widget.itemSelectionChanged.connect(self.dir_clicked)

        self.sim_list_widget = QListWidget()
        self.sim_list_widget.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)

        file_group_layout.addWidget(QLabel("디렉토리 목록:"))
        file_group_layout.addWidget(self.dir_list_widget)
        
        file_group_layout.addWidget(QLabel(".sim 파일 목록:"))
        file_group_layout.addWidget(self.sim_list_widget)
        
        self.file_group.setLayout(file_group_layout)
        self.left_layout.addWidget(self.file_group)

        # 작업 제출 버튼
        self.execute_button = QPushButton("작업 실행")
        self.execute_button.setEnabled(False)  # 초기 상태에서는 비활성화
        self.execute_button.clicked.connect(self.submit_job)
        self.left_layout.addWidget(self.execute_button)

    # 오른쪽 레이아웃 설정
    def setup_right_layout(self):
        self.java_combo_box = QComboBox()
        self.java_combo_box.currentIndexChanged.connect(self.java_combo_clicked)
        self.java_list_widget = QListWidget()
        self.java_list_widget.itemClicked.connect(self.java_clicked)
        self.java_list_widget.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)

        group_box = QGroupBox()
        layout = QVBoxLayout()

        layout.addWidget(QLabel(".java 일괄 선택:"))
        layout.addWidget(self.java_combo_box)

        layout.addWidget(QLabel(".java 파일 목록:"))
        layout.addWidget(self.java_list_widget)

        group_box.setLayout(layout)
        self.right_layout.addWidget(group_box)

    # 메뉴 생성
    def create_menu(self):
        menubar = self.menuBar()
        config_menu = menubar.addMenu('환경 설정')

        edit_action = QAction('환경 설정 값 변경', self)
        edit_action.triggered.connect(self.open_config_dialog)
        config_menu.addAction(edit_action)

    # 노드 레이아웃 생성
    def create_node_layout(self):
        layout = QHBoxLayout()
        layout.addWidget(self.node_label)
        layout.addWidget(self.create_node_button("+", self.increase_node_count))
        layout.addWidget(self.create_node_button("-", self.decrease_node_count))
        layout.addWidget(self.core_label)
        return layout

    # 노드 수량 증가/감소
    def increase_node_count(self):
        self.node_count += 1
        self.update_node_core_labels()

    def decrease_node_count(self):
        if self.node_count > 1:
            self.node_count -= 1
        self.update_node_core_labels()

    def update_node_core_labels(self):
        self.coretype = self.get_selected_coretype()
        self.cores_per_node = CORETYPE_CONFIGURATION.get(self.coretype, 0)
        self.node_label.setText(f"{self.coretype} : {self.node_count}")
        self.core_label.setText(f"    코어 수: {self.node_count * self.cores_per_node}")

    def get_selected_coretype(self):
        for radio_button in self.coretype_group.findChildren(QRadioButton):
            if radio_button.isChecked():
                return radio_button.text()
        return None

    # 구성 및 파일 로드
    def load_config(self):
        if path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)

            if config['apibaseurl'].endswith('/'):
                config['apibaseurl'] = config['apibaseurl'][:-1]
            return config
        else:
            QMessageBox.warning(self, "Warning", "config_miscellaneous.json이 없습니다.")
            exit(0)

    def save_config(self):
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.config, f, indent=4)

    def job_error(self, error):
        QMessageBox.critical(self, "Error", str(error[1]))


    # 이벤트 함수
    def dir_clicked(self):
        self.sim_list_widget.clear()
        self.java_list_widget.clear()
        self.java_combo_box.clear()
        selected_dirs = tuple(item.text() for item in self.dir_list_widget.selectedItems())

        if len(selected_dirs):
            self.sim_list_widget.addItems(tuple(sf for sf in self._sim_files if sf.split('\\')[0] in selected_dirs))
            for i in range(self.sim_list_widget.count()):
                self.sim_list_widget.item(i).setSelected(True)

            java_files = tuple(jf for jf in self._java_files if jf.split('\\')[0] in selected_dirs)
            self.java_list_widget.addItems(java_files)
            self.java_combo_box.addItem('Manual')
            self.java_combo_box.addItems(sorted(set(list(map(lambda j: j.split('\\')[-1], java_files)))))
            self.java_combo_clicked()

    def java_clicked(self, selected_item):
        dir = selected_item.text().split('\\')[0]
        for item in self.java_list_widget.selectedItems():
            if item is selected_item:
                continue
            elif dir in item.text():
                item.setSelected(False)

    def java_combo_clicked(self):
        current_text = self.java_combo_box.currentText()
        for i in range(self.java_list_widget.count()):
            if current_text == self.java_list_widget.item(i).text().split('\\')[-1]:
                self.java_list_widget.item(i).setSelected(True)
            else:
                self.java_list_widget.item(i).setSelected(False)


    # 작업 제출
    def submit_job(self):
        if not self.validate_inputs():
            return

        toText = lambda x: x.text()
        selected_sim_files = tuple(map(toText, self.sim_list_widget.selectedItems()))
        selected_java_files = tuple(map(toText, self.java_list_widget.selectedItems()))
        all_java_items = tuple(self.java_list_widget.item(i) for i in range(self.java_list_widget.count()))

        for dir in map(toText, self.dir_list_widget.selectedItems()):
            dir_java_file = tuple(filter(lambda x: dir in x, selected_java_files))[0] # CMD
            dir_sim_file = list(filter(lambda x: dir in x, selected_sim_files))[0] # CMD

            dirname = self._dir_dict[dir]['dirname']
            upload_files = [path.join(dirname, item) for item in map(toText, all_java_items) if dir in item] # Upload
            upload_files.append(path.join(dirname, dir_sim_file))

            # Submit the job
            submit_worker = SubmitWorker(
                self.get_selected_radio_button_text(self.job_type_group),
                self.config,
                self.extract_version_code(self.version_combo.currentText()),
                self.get_selected_radio_button_text(self.coretype_group),
                self.node_count * self.cores_per_node,
                self.walltime_combo.currentText(),
                upload_files,           # 업로드할 파일들
                dir_java_file,          # .java 파일 이름
                dir_sim_file,           # .sim 파일 이름
                self.log_signal,        # 로그 시그널 전달
            )
            submit_worker.signals.error.connect(self.job_error)
            self.job_threadpool.start(submit_worker)

    # 입력 파일 경로 설정
    def open_input_directory_dialog(self):
        input_directory = QFileDialog.getExistingDirectory(self, "입력 파일을 포함하는 폴더 선택")
        if input_directory:
            normalized_path = path.normpath(input_directory)
            self.input_directory_label.setText(normalized_path)
            # self.input_directory_button.setText(f'경로: {normalized_path}')
            self.update_file_list_widgets(normalized_path)
            
    # 선택된 경로에서 .sim 및 .java 파일을 검색하고 각 리스트 위젯에 표시
    def update_file_list_widgets(self, directory):
        self.dir_list_widget.clear()
        self.sim_list_widget.clear()  # 기존 항목 제거
        self.java_list_widget.clear()  # 기존 항목 제거
        self.java_combo_box.clear()  # 기존 항목 제거

        # .sim 및 .java 파일 검색
        dir_dict = {}
        for root, _, files in walk(directory):
            dir = path.basename(root)
            dir_dict[dir] = {'dirname': path.dirname(root), 'simfiles': [], 'javafiles': []}
            hasfile = False
            for file in files:
                if file.endswith(".sim"):
                    dir_dict[dir]['simfiles'].append(dir + '\\' + file)
                    hasfile = True
                elif file.endswith(".java"):
                    dir_dict[dir]['javafiles'].append(dir + '\\' + file)
                    hasfile = True
            if not hasfile:
                del dir_dict[dir]
        self._dir_dict = dir_dict

        # 파일 목록을 각 list_widget에 추가
        self.dir_list_widget.addItems(dir_dict.keys())
        self._sim_files = sorted(sum([v['simfiles'] for k, v in dir_dict.items()], []))
        self.sim_list_widget.addItems(self._sim_files)
        self._java_files = sorted(sum([v['javafiles'] for k, v in dir_dict.items()], []))
        self.java_list_widget.addItems(self._java_files)

        if len(self._java_files):
            self.java_combo_box.addItem('Manual')
            self.java_combo_box.addItems(sorted(set(list(map(lambda j: j.split('\\')[-1], self._java_files)))))

        # 기본적으로 모든 디렉토리를 선택
        for index in range(self.dir_list_widget.count()):
            self.dir_list_widget.item(index).setSelected(True)

        # 기본적으로 모든 .sim 파일을 선택
        for index in range(self.sim_list_widget.count()):
            self.sim_list_widget.item(index).setSelected(True)

        # 파일이 발견되면 실행 버튼 활성화
        if len(self._sim_files) and len(self._java_files):
            self.execute_button.setEnabled(True)
        else:
            self.execute_button.setEnabled(False)

    # 기타 유틸리티 메서드
    def validate_inputs(self):
        if len(self.dir_list_widget.selectedItems()) == 0:
            QMessageBox.warning(self, "Warning", "선택된 디렉토리가 없습니다.")
            return False

        toText = lambda x: x.text()
        for dir in map(toText, self.dir_list_widget.selectedItems()):
            sim_cnt = sum([1 for sim in map(toText, self.sim_list_widget.selectedItems()) if dir in sim])
            if sim_cnt != 1:
                if sim_cnt == 0:
                    msg = f"{dir}에 선택된 sim 파일이 없습니다."
                else:
                    msg = f"{dir}에 선택된 sim 파일이 너무 많습니다."
                QMessageBox.warning(self, "Warning", msg)
                return

            java_cnt = sum([1 for java in map(toText, self.java_list_widget.selectedItems()) if dir in java])
            if java_cnt != 1:
                if java_cnt == 0:
                    msg = f"{dir}에 선택된 java 파일이 없습니다."
                else:
                    msg = f"{dir}에 선택된 java 파일이 너무 많습니다."
                QMessageBox.warning(self, "Warning", msg)
                return
        return True

    # 선택한 메뉴에서 소프트웨어 버전 정보 추출
    def extract_version_code(self, version_text):
        version = version_text.split(' ')[0]
        if "Double Precision" in version_text:
            version += "-r8"
        return f"{version}-HKMC-aerot-231207"

    def open_config_dialog(self):
        config_dialog = ConfigDialog(CONFIG_FILE, self)
        if config_dialog.exec():
            self.config = config_dialog.config

    # UI 구성 도우미 메서드
    def create_radio_group(self, title, options):
        group_box = QGroupBox(title)
        layout = QHBoxLayout()
        for option in options:
            radio_button = QRadioButton(option)
            layout.addWidget(radio_button)
        layout.itemAt(0).widget().setChecked(True)
        group_box.setLayout(layout)
        return group_box

    def create_node_control(self):
        coretype = self.get_selected_coretype()
        node_label = QLabel(f"{coretype} : {DEFAULT_NODE_COUNT}")
        core_label = QLabel(f"    코어 수: {DEFAULT_NODE_COUNT * CORETYPE_CONFIGURATION[coretype]}")
        return node_label, core_label

    def create_node_button(self, text, func):
        button = QPushButton(text)
        button.clicked.connect(func)
        return button

    def get_selected_radio_button_text(self, group_box):
        for button in group_box.findChildren(QRadioButton):
            if button.isChecked():
                return button.text()
        return ""
