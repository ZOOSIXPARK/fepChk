import streamlit as st
import pandas as pd
import sqlite3
import os
import io
from datetime import datetime

# --- 설정 및 초기화 ---
DB_NAME = "fep_rms_test.db"
CSV_FILE = "fep.csv"

def init_db():
    """데이터베이스 테이블 초기화"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS test_results (
            rms_dept TEXT,
            external_inst TEXT,
            is_tested INTEGER,
            prod_reflection_date TEXT,
            updated_at TEXT,
            PRIMARY KEY (rms_dept, external_inst)
        )
    ''')
    conn.commit()
    conn.close()

def load_fep_data():
    """fep.csv 파일을 읽어서 RMS 기준 딕셔너리로 변환"""
    if os.path.exists(CSV_FILE):
        try:
            try:
                df = pd.read_csv(CSV_FILE, encoding='utf-8')
            except:
                df = pd.read_csv(CSV_FILE, encoding='cp949')

            if '내부업체' in df.columns:
                df = df.rename(columns={'내부업체': 'RMS'})
            
            if 'RMS' not in df.columns:
                st.error("CSV 파일에 'RMS' 또는 '내부업체' 컬럼이 필요합니다.")
                return None

            # 결측치(빈 줄) 제거 및 문자열 강제 변환 (오류 방지)
            df = df.dropna(subset=['RMS', '대외기관'])
            df['RMS'] = df['RMS'].astype(str)
            df['대외기관'] = df['대외기관'].astype(str)

            mapped_data = df.groupby('RMS')['대외기관'].apply(list).to_dict()
            return mapped_data
        except Exception as e:
            st.error(f"CSV 로드 오류: {e}")
            return None
    else:
        st.warning(f"'{CSV_FILE}' 파일이 없습니다. RMS와 대외기관 정보가 담긴 CSV를 준비해주세요.")
        return None

def save_data(rms_dept, results):
    """DB 저장 (Upsert)"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for inst, data in results.items():
        cursor.execute('''
            INSERT OR REPLACE INTO test_results 
            (rms_dept, external_inst, is_tested, prod_reflection_date, updated_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (rms_dept, inst, 1 if data['tested'] else 0, data['prod_reflection_date'], now))
    
    conn.commit()
    conn.close()

def get_all_results():
    """DB 결과 전체 조회"""
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM test_results", conn)
    conn.close()
    return df

def get_results_by_rms(rms_dept):
    """특정 RMS 부서의 기존 저장 내역 조회"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT external_inst, is_tested, prod_reflection_date FROM test_results WHERE rms_dept = ?", (rms_dept,))
    rows = cursor.fetchall()
    conn.close()
    return {row[0]: {'is_tested': row[1], 'date': row[2]} for row in rows}

def get_kpi_metrics(mapping):
    """KPI 지표 집계"""
    total_rms_count = len(mapping) if mapping else 0
    total_target_count = sum(len(insts) for insts in mapping.values()) if mapping else 0

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT rms_dept) FROM test_results")
    row_rms = cursor.fetchone()
    participated_rms_count = row_rms[0] if row_rms else 0
    
    cursor.execute("SELECT COUNT(*) FROM test_results WHERE is_tested = 1")
    row_test = cursor.fetchone()
    completed_test_count = row_test[0] if row_test else 0
    conn.close()

    return total_rms_count, participated_rms_count, total_target_count, completed_test_count

def convert_df_to_excel(df):
    """DataFrame -> 엑셀 변환"""
    output = io.BytesIO()
    export_df = df.copy()
    export_df['is_tested'] = export_df['is_tested'].map({1: "완료", 0: "미완료"})
    export_df.columns = ["RMS", "대외기관", "테스트상태", "운영 반영일정", "최종갱신시간"]
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='전체점검내역')
    return output.getvalue()

# --- 메인 UI ---
def main():
    st.set_page_config(page_title="KB증권)대외계-RMS 분리 작업", layout="wide")
    init_db()
    
    st.title("🛡️ KB증권)대외계-RMS 분리 작업 점검 시스템")
    
    mapping = load_fep_data()
    all_df = get_all_results() 
    
    # --- 상단 KPI 대시보드 ---
    if mapping:
        st.markdown("### 📊 진행 현황 요약")
        total_rms, part_rms, total_target, comp_test = get_kpi_metrics(mapping)
        
        rms_progress = (part_rms / total_rms * 100) if total_rms > 0 else 0
        test_progress = (comp_test / total_target * 100) if total_target > 0 else 0

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric(label="전체 대상 RMS 부서", value=f"{total_rms} 개")
        kpi2.metric(label="운영반영 확인 RMS", value=f"{part_rms} 개", delta=f"진행률 {rms_progress:.1f}%")
        kpi3.metric(label="전체 대외기관 테스트 대상", value=f"{total_target} 건")
        kpi4.metric(label="테스트 완료 건수", value=f"{comp_test} 건", delta=f"진척률 {test_progress:.1f}%")
        st.markdown("---")

    # --- 본문 영역 ---
    if mapping:
        col1, col2 = st.columns([1, 1.2])
        
        with col1:
            st.subheader("📝 점검 내역 입력 (조회/수정)")
            rms_list = list(mapping.keys())
            selected_rms = st.selectbox("점검 대상 RMS를 선택하세요:", rms_list)
            
            institutions = mapping[selected_rms]
            st.write(f"📌 **{selected_rms}** 연계 기관 ({len(institutions)}건)")
            
            # DB 데이터를 Streamlit Session State(메모리)에 동기화
            existing_data = get_results_by_rms(selected_rms)
            
            for inst in institutions:
                chk_key = f"chk_{selected_rms}_{inst}" 
                date_key = f"date_{selected_rms}_{inst}"
                
                if chk_key not in st.session_state:
                    st.session_state[chk_key] = bool(existing_data.get(inst, {}).get('is_tested', False))
                    
                if date_key not in st.session_state:
                    saved_date_str = existing_data.get(inst, {}).get('date', "")
                    default_date = None
                    if saved_date_str:
                        try:
                            default_date = datetime.strptime(saved_date_str, "%Y-%m-%d").date()
                        except ValueError:
                            pass
                    st.session_state[date_key] = default_date

            # 일괄 지정 달력 상태 관리
            bulk_state_key = f"bulk_state_{selected_rms}"
            if bulk_state_key not in st.session_state:
                st.session_state[bulk_state_key] = None

            ui_bulk_date = st.date_input(
                "💡 운영 반영일정 일괄 지정 (선택 시 아래 목록에 자동 덮어쓰기 됩니다)", 
                value=None, 
                key=f"ui_bulk_{selected_rms}"
            )

            if ui_bulk_date != st.session_state[bulk_state_key]:
                st.session_state[bulk_state_key] = ui_bulk_date
                if ui_bulk_date is not None:
                    for inst in institutions:
                        st.session_state[f"date_{selected_rms}_{inst}"] = ui_bulk_date
                st.rerun()

            # 입력 폼 렌더링
            with st.form(key=f"form_{selected_rms}"):
                for inst in institutions:
                    st.markdown(f"**{inst}**")
                    c1, c2 = st.columns([1, 3])
                    
                    chk_key = f"chk_{selected_rms}_{inst}"
                    date_key = f"date_{selected_rms}_{inst}"
                    
                    with c1:
                        st.checkbox("테스트 완료", key=chk_key)
                    with c2:
                        st.date_input("운영 반영일정", key=date_key)
                        
                # 버튼 명칭 '결과저장'으로 변경
                submit_btn = st.form_submit_button("결과저장", use_container_width=True)
                
                if submit_btn:
                    current_inputs = {}
                    for inst in institutions:
                        is_done = st.session_state[f"chk_{selected_rms}_{inst}"]
                        selected_date = st.session_state[f"date_{selected_rms}_{inst}"]
                        date_str = selected_date.strftime("%Y-%m-%d") if selected_date else ""
                        current_inputs[inst] = {"tested": is_done, "prod_reflection_date": date_str}
                    
                    save_data(selected_rms, current_inputs)
                    st.success("저장 완료!")
                    st.rerun()

        with col2:
            st.subheader("📋 실시간 점검 현황")
            if not all_df.empty:
                display_df = all_df.copy()
                display_df['is_tested'] = display_df['is_tested'].map({1: "✅ 완료", 0: "⏳ 미완료"})
                display_df.columns = ["RMS", "대외기관", "상태", "운영 반영일정", "업데이트 시간"]
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("아직 저장된 점검 결과가 없습니다.")

    # --- 하단 엑셀 다운로드 ---
    st.markdown("<br><br>", unsafe_allow_html=True) 
    st.markdown("---")
    st.subheader("📥 전체 데이터 내보내기")
    
    all_df_latest = get_all_results() 
    if not all_df_latest.empty:
        excel_data = convert_df_to_excel(all_df_latest)
        st.download_button(
            label="📊 전체 진행내역 엑셀 다운로드 (클릭 시 다운로드 시작)",
            data=excel_data,
            file_name=f"RMS_분리작업_전체점검내역_{datetime.now().strftime('%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    else:
        st.info("💡 아직 데이터베이스에 저장된 점검 결과가 없어 다운로드할 수 없습니다.")

if __name__ == "__main__":
    main()