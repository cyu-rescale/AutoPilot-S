import os
import json



# 통합 작업: HBv4
def create_job_hbv4(file_ids, java_file_name, sim_file_name, jobname, software, version_code, 
                    license_server, nslots_basic, coretype, ncores, walltime, project_code):
    job_data = {
        'isLowPriority': False,
        'name': jobname,
        'jobanalyses': [
            {
                'envVars': {'CDLMD_LICENSE_FILE': license_server},
                'useRescaleLicense': 'false',
                'onDemandLicenseSeller': '',
                'userDefinedLicenseSettings': {
                    'featureSets': [{
                        'name': 'USER_SPECIFIED',
                        'features': [{'name': 'ccmppower', 'count': '1'}]
                    }]
                },
                'command': ('export STARTING_TIME=$(date +"%Y%m%d_%H%M%S")\n'
                            'export MPI_FLAVOR=platformmpi\n'
                            'export user_override_microsoft_infiniband_v4_platformmpi="-TCP"\n'
                            f'starccm+ -power -np $RESCALE_CORES_PER_SLOT -batch {java_file_name} '
                            f'-load $(realpath {sim_file_name}) | tee "${{STARTING_TIME}}-${{RESCALE_JOB_ID}}.log"\n'
                            rf'find . -type d -name "*_Mesh" -o '
                            rf'-type f \( -name "*_Mesh.sim" -o -name "*_Mesh_ESV_Mode.sim" \) '
                            rf'-print | zip -s 4g "${{STARTING_TIME}}-${{RESCALE_JOB_ID}}_results.zip" -@'),
                'analysis': {'code': software, 'version': version_code},
                'hardware': {
                    'coresPerSlot': ncores,
                    'walltime': walltime,
                    'slots': nslots_basic,
                    'coreType': coretype,
                },
                'inputFiles': [{'id': file_id} for file_id in file_ids],
            }
        ],
        'projectId': project_code,
    }
    return job_data    


# 통합 작업: HBv3
def create_job_hbv3(file_ids, java_file_name, sim_file_name, jobname, software, version_code,
                    license_server, nslots_basic, coretype, ncores, walltime, project_code):
    job_data = {
        'isLowPriority': False,
        'name': jobname,
        'jobanalyses': [
            {
                'envVars': {'CDLMD_LICENSE_FILE': license_server},
                'useRescaleLicense': 'false',
                'onDemandLicenseSeller': '',
                'userDefinedLicenseSettings': {
                    'featureSets': [{
                        'name': 'USER_SPECIFIED',
                        'features': [{'name': 'ccmppower', 'count': '1'}]
                    }]
                },
                'command': ('export STARTING_TIME=$(date +"%Y%m%d_%H%M%S")\n'
                            'export MPI_FLAVOR=platformmpi\n'
                            f'starccm+ -power -np $RESCALE_CORES_PER_SLOT -batch {java_file_name} '
                            f'-load $(realpath {sim_file_name}) | tee "${{STARTING_TIME}}-${{RESCALE_JOB_ID}}.log"\n'
                            rf'find . -type d -name "*_Mesh" -o '
                            rf'-type f \( -name "*_Mesh.sim" -o -name "*_Mesh_ESV_Mode.sim" \) '
                            rf'-print | zip -s 4g "${{STARTING_TIME}}-${{RESCALE_JOB_ID}}_results.zip" -@'),
                'analysis': {'code': software, 'version': version_code},
                'hardware': {
                    'coresPerSlot': ncores,
                    'walltime': walltime,
                    'slots': nslots_basic,
                    'coreType': coretype,
                },
                'inputFiles': [{'id': file_id} for file_id in file_ids],
            }
        ],
        'projectId': project_code,
    }
    return job_data


# Rescale internal: 테스트 작업 실행을 위한 라이선스 정보 획득 함수
def load_starccmp_config():
    # 사용자 홈 디렉토리 경로 확장
    config_file_path = os.path.expanduser("~/.config/rescale/starccmp.json")

    # starccmp.json 파일이 존재하는지 확인
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"{config_file_path} 파일을 찾을 수 없습니다.")

    # JSON 파일 읽기
    with open(config_file_path, 'r', encoding='utf-8') as file:
        config_data = json.load(file)

    # CDLMD_LICENSE_FILE과 LM_PROJECT 값을 추출하여 변수에 저장
    cdlmd_license_file = config_data.get("CDLMD_LICENSE_FILE")
    lm_project = config_data.get("LM_PROJECT")

    # 값이 없는 경우 에러 처리
    if cdlmd_license_file is None:
        raise ValueError("CDLMD_LICENSE_FILE 값을 찾을 수 없습니다.")
    if lm_project is None:
        raise ValueError("LM_PROJECT 값을 찾을 수 없습니다.")

    return cdlmd_license_file, lm_project

# Rescale internal: 테스트 작업 생성 함수
def create_job_test(file_ids, java_file_name, sim_file_name, jobname, software, version_code,
                    license_server, nslots_basic, coretype, ncores, walltime, project_code):
    cdlmd_license_file, lm_project = load_starccmp_config()

    job_data = {
        'isLowPriority': False,
        'name': jobname,
        'jobanalyses': [
            {
                'envVars': {'CDLMD_LICENSE_FILE': cdlmd_license_file, 'LM_PROJECT': lm_project},
                'useRescaleLicense': 'false',
                'onDemandLicenseSeller': '',
                'command': ('export STARTING_TIME=$(date +"%Y%m%d_%H%M%S")\n'
                            'export MPI_FLAVOR=platformmpi\n'
                            f'starccm+ -power -np $RESCALE_CORES_PER_SLOT -batch run '
                            f'-load $(realpath {sim_file_name}) | tee "${{STARTING_TIME}}-${{RESCALE_JOB_ID}}.log"\n'
                            'find . -type f -name "*.sim" -print | zip -s 4m "${STARTING_TIME}-${RESCALE_JOB_ID}_results.zip" -@\n'
                            'rm *.sim'),
                'analysis': {'code': software, 'version': version_code},
                'hardware': {
                    'coresPerSlot': ncores,
                    'walltime': walltime,
                    'slots': nslots_basic,
                    'coreType': coretype,
                },
                'inputFiles': [{'id': file_id} for file_id in file_ids],
            }
        ],
        'projectId': project_code,
    }
    return job_data