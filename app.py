import streamlit as st
import pandas as pd
import os
import io
from datetime import datetime
from sqlalchemy import text

# --- 설정 ---
CSV_FILE = "fep.csv"

def load_fep_data():
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
            return df.groupby('RMS')['대외기관'].apply(list).to_dict()
        except Exception as e:
            st.error(f"CSV 로드 오류: {e}")
            return None
    else:
        st.warning(f"'{CSV_FILE}' 파일이 없습니다.")
        return None

def save_data(rms_dept, manager_name, results):
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
            s.execute(sql, {"rms": rms_dept, "inst": inst, "tested": 1 if data['tested'] else 0, "date": data['prod_reflection_date'], "manager": manager_name, "updated": now})
        s.commit()

def get_all_results():
    conn = st.connection("supabase", type="sql")
    return conn.query("SELECT rms_dept, external_inst, is_tested, prod_reflection_date, manager, updated_at FROM test_results", ttl=0)

def get_results_by_rms(rms_dept):
    conn = st.connection("supabase", type="sql")
    sql = text("SELECT external_inst, is_tested, prod_reflection_date, manager FROM test_results WHERE rms_dept = :rms")
    with conn.session as s:
        try:
            result = s.execute(sql, {"rms": rms_dept})
            rows = result.fetchall()
            return {row[0]: {'is_tested': row[1], 'date': row[2], 'manager': row[3]} for row in rows}
        except:
            sql_fallback = text("SELECT external_inst, is_tested, prod_reflection_date FROM test_results WHERE rms_dept = :rms")
            result = s.execute(sql_fallback, {"rms": rms_dept})
            rows = result.fetchall()
            return {row[0]: {'is_tested': row[1], 'date': row[2], 'manager': ''} for row in rows}

def main():
    st.set_page_config(page_title="KB증권 대외계-RMS 분리 작업 대시보드", layout="wide")
    st.title("KB증권 대외계-RMS 분리 작업 대시보드")
    
    mapping = load_fep_data()
    all_df = get_all_results() 
    
    if mapping:
        col1, col2 = st.columns([1, 1.2])
        with col1:
            st.subheader("📝 테스트 점검 및 운영반영 일정 입력")
            selected_rms = st.selectbox("점검 대상 RMS 업체명 선택:", list(mapping.keys()))
            institutions = mapping[selected_rms]
            existing_data = get_results_by_rms(selected_rms)
            
            for inst in institutions:
                chk_key, date_key = f"chk_{selected_rms}_{inst}", f"date_{selected_rms}_{inst}"
                if chk_key not in st.session_state:
                    st.session_state[chk_key] = bool(existing_data.get(inst, {}).get('is_tested', False))
                if date_key not in st.session_state:
                    saved_date = existing_data.get(inst, {}).get('date', "")
                    st.session_state[date_key] = datetime.strptime(saved_date, "%Y-%m-%d").date() if saved_date else None

            ui_bulk = st.date_input("💡 운영 반영일정 일괄 지정", value=None)
            if ui_bulk:
                for inst in institutions:
                    st.session_state[f"date_{selected_rms}_{inst}"] = ui_bulk

            manager_name = st.text_input("👤 작성자", value=existing_data.get(institutions[0], {}).get('manager', "") if institutions and existing_data else "", key=f"manager_{selected_rms}")

            with st.form(key=f"form_{selected_rms}"):
                for inst in institutions:
                    st.markdown(f"#### 🔹 {inst.strip()}")
                    st.checkbox("개발통신 확인 및 테스트 점검 완료", key=f"chk_{selected_rms}_{inst}")
                    st.date_input("운영 반영일정", key=f"date_{selected_rms}_{inst}")
                    st.markdown("<hr>", unsafe_allow_html=True)
                
                if st.form_submit_button("결과저장", type="primary", use_container_width=True):
                    res = {inst: {"tested": st.session_state[f"chk_{selected_rms}_{inst}"], 
                                  "prod_reflection_date": st.session_state[f"date_{selected_rms}_{inst}"].strftime("%Y-%m-%d") if st.session_state[f"date_{selected_rms}_{inst}"] else ""} 
                           for inst in institutions}
                    save_data(selected_rms, manager_name, res)
                    st.success("저장 완료!")
                    st.rerun()

        with col2:
            st.subheader("📋 점검 내역")
            # 선택된 RMS 업체명으로 필터링
            disp_df = all_df[all_df['rms_dept'] == selected_rms].copy()
            
            if not disp_df.empty:
                disp_df['is_tested'] = disp_df['is_tested'].map({1: "✅ 완료", 0: "⏳ 미완료"})
                disp_df.columns = ["RMS", "대외기관", "상태", "운영 반영일정", "작성자", "업데이트 시간"]
                st.dataframe(disp_df, use_container_width=True, hide_index=True)
            else:
                st.info("해당 RMS 부서에 대한 점검 내역이 없습니다.")

if __name__ == "__main__":
    main()
