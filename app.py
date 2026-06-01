import streamlit as st
import pandas as pd
import os
import io
from datetime import datetime
from sqlalchemy import text

# --- 설정 ---
CSV_FILE = "fep.csv"

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

def save_data(rms_dept, manager_name, results):
    """Supabase DB 저장 (PostgreSQL Upsert 적용)"""
    conn = st.connection("supabase", type="sql")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with conn.session as s:
        for inst, data in results.items():
            sql = text('''
                INSERT INTO test_results 
                (rms_dept, external_inst, is_tested, prod_reflection_date, manager, updated_at)
                VALUES (:rms, :inst, :tested, :date, :manager, :updated)
                ON CONFLICT (rms_dept, external_inst) 
                DO UPDATE SET 
                    is_tested = EXCLUDED.is_tested,
                    prod_reflection_date = EXCLUDED.prod_reflection_date,
                    manager = EXCLUDED.manager,
                    updated_at = EXCLUDED.updated_at
            ''')
            
            s.execute(sql, {
                "rms": rms_dept, 
                "inst": inst, 
                "tested": 1 if data['tested'] else 0, 
                "date": data['prod_reflection_date'], 
                "manager": manager_name,
                "updated": now
            })
        s.commit()

def get_all_results():
    conn = st.connection("supabase", type="sql")
    return conn.query("SELECT rms_dept, external_inst, is_tested, prod_reflection_date, manager, updated_at FROM test_results", ttl=0)

def get_results_by_rms(rms_dept):
    conn = st.connection("supabase", type="sql")
    sql = text("SELECT external_inst, is_tested, prod_reflection_date, manager FROM test_results WHERE rms_dept = :rms")
    with conn.session as s:
        result = s.execute(sql, {"rms": rms_dept})
        rows = result.fetchall()
        return {row[0]: {'is_tested': row[1], 'date': row[2], 'manager': row[3]} for row in rows}

def convert_df_to_excel(df):
    output = io.BytesIO()
    export_df = df.copy()
    export_df['is_tested'] = export_df['is_tested'].map({1: "완료", 0: "미완료"})
    export_df.columns = ["RMS", "대외기관", "상태", "운영 반영일정", "작성자", "최종갱신시간"]
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='전체점검내역')
    return output.getvalue()

def main():
    st.set_page_config(page_title="KB증권 대외계-RMS 분리 작업 대시보드", layout="wide")
    st.title("KB증권 대외계-RMS 분리 작업 관련 대시보드")
    st.info("💡 테스트 확인 후 운영 반영 일정을 입력 및 수정할 수 있습니다.")
    
    mapping = load_fep_data()
    all_df = get_all_results()
    
    if mapping:
        col1, col2 = st.columns([1, 1.2])
        with col1:
            selected_rms = st.selectbox("점검 대상 RMS 업체명 선택:", list(mapping.keys()))
            institutions = mapping[selected_rms]
            existing_data = get_results_by_rms(selected_rms)
            
            # Session State 초기화
            for inst in institutions:
                chk_key, date_key = f"chk_{selected_rms}_{inst}", f"date_{selected_rms}_{inst}"
                if chk_key not in st.session_state:
                    st.session_state[chk_key] = bool(existing_data.get(inst, {}).get('is_tested', False))
                if date_key not in st.session_state:
                    saved_date = existing_data.get(inst, {}).get('date', "")
                    st.session_state[date_key] = datetime.strptime(saved_date, "%Y-%m-%d").date() if saved_date else None

            # 일괄 지정 달력 (활성화)
            ui_bulk = st.date_input("💡 운영 반영일정 일괄 지정", value=None)
            if ui_bulk:
                for inst in institutions:
                    st.session_state[f"date_{selected_rms}_{inst}"] = ui_bulk

            manager_name = st.text_input("👤 작성자", value=existing_data.get(institutions[0], {}).get('manager', ""))

            with st.form(key=f"form_{selected_rms}"):
                for inst in institutions:
                    st.markdown(f"**🔹 {inst}**")
                    st.checkbox("테스트 점검 완료", key=f"chk_{selected_rms}_{inst}")
                    st.date_input("운영 반영일정", key=f"date_{selected_rms}_{inst}") # 활성화됨
                
                if st.form_submit_button("결과저장", type="primary"):
                    res = {inst: {
                        "tested": st.session_state[f"chk_{selected_rms}_{inst}"], 
                        "prod_reflection_date": st.session_state[f"date_{selected_rms}_{inst}"].strftime("%Y-%m-%d") if st.session_state[f"date_{selected_rms}_{inst}"] else None
                    } for inst in institutions}
                    save_data(selected_rms, manager_name, res)
                    st.success("저장 완료!")
                    st.rerun()

        with col2:
            st.subheader("📋 점검 내역")
            st.dataframe(all_df, use_container_width=True)

if __name__ == "__main__":
    main()
