import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
from sqlalchemy import text

# --- 설정 ---
CSV_FILE = "fep.csv"

def get_kst_now():
    """한국 시간(KST)을 반환합니다."""
    return datetime.now(timezone(timedelta(hours=9)))

def load_fep_data():
    if not os.path.exists(CSV_FILE):
        st.warning(f"'{CSV_FILE}' 파일이 없습니다.")
        return None
    try:
        try:
            df = pd.read_csv(CSV_FILE, encoding='utf-8')
        except:
            df = pd.read_csv(CSV_FILE, encoding='cp949')
        
        if '내부업체' in df.columns:
            df = df.rename(columns={'내부업체': 'RMS'})
        if 'RMS' not in df.columns or '대외기관' not in df.columns:
            st.error("CSV 파일에 'RMS'와 '대외기관' 컬럼이 필요합니다.")
            return None
            
        df = df.dropna(subset=['RMS', '대외기관'])
        df['RMS'] = df['RMS'].astype(str)
        df['대외기관'] = df['대외기관'].astype(str)
        return df.groupby('RMS')['대외기관'].apply(list).to_dict()
    except Exception as e:
        st.error(f"CSV 로드 오류: {e}")
        return None

def save_data(rms_dept, manager_name, results):
    conn = st.connection("supabase", type="sql")
    now = get_kst_now().strftime("%Y-%m-%d %H:%M:%S")
    
    with conn.session as s:
        for inst, data in results.items():
            sql = text('''
                INSERT INTO test_results 
                (rms_dept, external_inst, is_tested, prod_reflection_date, prod_days, manager, updated_at)
                VALUES (:rms, :inst, :tested, :date, :prod_days, :manager, :updated)
                ON CONFLICT (rms_dept, external_inst) 
                DO UPDATE SET 
                    is_tested = EXCLUDED.is_tested,
                    prod_reflection_date = EXCLUDED.prod_reflection_date,
                    prod_days = EXCLUDED.prod_days,
                    manager = EXCLUDED.manager,
                    updated_at = EXCLUDED.updated_at
            ''')
            s.execute(sql, {
                "rms": rms_dept, 
                "inst": inst, 
                "tested": 1 if data['tested'] else 0, 
                "date": data['prod_reflection_date'], 
                "prod_days": data['prod_days'],
                "manager": manager_name, 
                "updated": now
            })
        s.commit()
    st.cache_data.clear() # 데이터 갱신을 위해 캐시 초기화

def get_all_results():
    conn = st.connection("supabase", type="sql")
    return conn.query("SELECT rms_dept, external_inst, is_tested, prod_reflection_date, prod_days, manager, updated_at FROM test_results", ttl=0)

def get_results_by_rms(rms_dept):
    conn = st.connection("supabase", type="sql")
    sql = text("SELECT external_inst, is_tested, prod_reflection_date, prod_days, manager FROM test_results WHERE rms_dept = :rms")
    with conn.session as s:
        result = s.execute(sql, {"rms": rms_dept})
        return {row[0]: {'is_tested': row[1], 'date': row[2], 'prod_days': row[3], 'manager': row[4]} for row in result.fetchall()}

def main():
    st.set_page_config(page_title="KB증권 대외계-RMS 분리 작업 대시보드", layout="wide")
    st.title("KB증권 대외계-RMS 분리 작업 대시보드")
    
    mapping = load_fep_data()
    if not mapping: return
    
    all_df = get_all_results() 
    
    col1, col2 = st.columns([1, 1.2])
    with col1:
        st.subheader("📝 테스트 점검 및 운영반영 일정 입력")
        selected_rms = st.selectbox("점검 대상 RMS 업체명 선택:", list(mapping.keys()))
        institutions = mapping[selected_rms]
        existing_data = get_results_by_rms(selected_rms)
        
        # 일괄 반영 UI (step=60 추가로 1분 단위 설정 지원)
        ui_bulk = st.date_input("💡 운영 반영일정 일괄 지정 (선택 시 하단 적용)", value=None)
        ui_bulk_time = st.time_input("💡 운영 반영시간 일괄 지정 (선택 시 하단 적용)", value=None, step=60)
        
        # 작성자 정보
        manager_name = st.text_input("👤 작성자", value=list(existing_data.values())[0]['manager'] if existing_data else "")

        with st.form(key=f"form_{selected_rms}"):
            res_dict = {}
            for inst in institutions:
                st.markdown(f"#### 🔹 {inst.strip()}")
                data = existing_data.get(inst, {})
                
                checked = st.checkbox("개발통신 확인 및 테스트 점검 완료", value=bool(data.get('is_tested')), key=f"chk_{inst}")
                
                # 일괄 날짜가 선택되었거나 기존 날짜가 있는 경우 설정
                default_date = ui_bulk if ui_bulk else (pd.to_datetime(data.get('date')).date() if data.get('date') else None)
                date_val = st.date_input("운영 반영일정", value=default_date, key=f"date_{inst}")
                
                # 일괄 시간이 선택되었거나 기존 시간이 있는 경우 설정
                existing_time_str = data.get('prod_days')
                try:
                    existing_time = pd.to_datetime(existing_time_str).time() if existing_time_str else None
                except:
                    existing_time = None
                    
                default_time = ui_bulk_time if ui_bulk_time else existing_time
                # step=60 추가로 1분 단위 설정 지원
                time_val = st.time_input("운영 반영시간", value=default_time, key=f"time_{inst}", step=60)
                
                res_dict[inst] = {
                    "tested": checked, 
                    "prod_reflection_date": str(date_val) if date_val else "",
                    "prod_days": time_val.strftime("%H:%M") if time_val else ""
                }
                st.markdown("<hr>", unsafe_allow_html=True)
            
            if st.form_submit_button("결과저장", type="primary", use_container_width=True):
                save_data(selected_rms, manager_name, res_dict)
                st.success("저장 완료!")
                st.rerun()

    with col2:
        st.subheader("📋 점검 내역")
        disp_df = all_df[all_df['rms_dept'] == selected_rms].copy()
        if not disp_df.empty:
            disp_df['is_tested'] = disp_df['is_tested'].map({1: "✅ 완료", 0: "⏳ 미완료"})
            # DataFrame에 추가된 컬럼 반영
            disp_df.columns = ["RMS", "대외기관", "상태", "운영 반영일정", "운영 반영시간", "작성자", "업데이트 시간"]
            st.dataframe(disp_df, use_container_width=True, hide_index=True)
        else:
            st.info("해당 RMS 부서에 대한 점검 내역이 없습니다.")

if __name__ == "__main__":
    main()
