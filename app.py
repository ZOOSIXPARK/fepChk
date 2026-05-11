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

def save_data(rms_dept, results):
    """Supabase DB 저장 (PostgreSQL Upsert 적용)"""
    conn = st.connection("supabase", type="sql")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with conn.session as s:
        for inst, data in results.items():
            sql = text('''
                INSERT INTO test_results 
                (rms_dept, external_inst, is_tested, prod_reflection_date, updated_at)
                VALUES (:rms, :inst, :tested, :date, :updated)
                ON CONFLICT (rms_dept, external_inst) 
                DO UPDATE SET 
                    is_tested = EXCLUDED.is_tested,
                    prod_reflection_date = EXCLUDED.prod_reflection_date,
                    updated_at = EXCLUDED.updated_at
            ''')
            
            s.execute(sql, {
                "rms": rms_dept, 
                "inst": inst, 
                "tested": 1 if data['tested'] else 0, 
                "date": data['prod_reflection_date'], 
                "updated": now
            })
        s.commit()

def get_all_results():
    """Supabase DB 결과 전체 조회"""
    conn = st.connection("supabase", type="sql")
    return conn.query("SELECT * FROM test_results", ttl=0)

def get_results_by_rms(rms_dept):
    """특정 RMS 부서의 기존 저장 내역 조회"""
    conn = st.connection("supabase", type="sql")
    sql = text("SELECT external_inst, is_tested, prod_reflection_date FROM test_results WHERE rms_dept = :rms")
    
    with conn.session as s:
        result = s.execute(sql, {"rms": rms_dept})
        rows = result.fetchall()
        
    return {row[0]: {'is_tested': row[1], 'date': row[2]} for row in rows}

def get_kpi_metrics(mapping):
    """KPI 지표 집계"""
    total_rms_count = len(mapping) if mapping else 0
    total_target_count = sum(len(insts) for insts in mapping.values()) if mapping else 0

    conn = st.connection("supabase", type="sql")
    
    with conn.session as s:
        row_rms = s.execute(text("SELECT COUNT(DISTINCT rms_dept) FROM test_results")).fetchone()
        part_rms = row_rms[0] if row_rms else 0
        
        row_test = s.execute(text("SELECT COUNT(*) FROM test_results WHERE is_tested = 1")).fetchone()
        comp_test = row_test[0] if row_test else 0

    return total_rms_count, part_rms, total_target_count, comp_test

def convert_df_to_excel(df):
    """DataFrame -> 엑셀 변환"""
    output = io.BytesIO()
    export_df = df.copy()
    export_df['is_tested'] = export_df['is_tested'].map({1: "완료", 0: "미완료"})
    export_df.columns = ["RMS", "대외기관", "상태", "운영 반영일정", "최종갱신시간"]
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='전체점검내역')
    return output.getvalue()

# --- 메인 UI ---
def main():
    st.set_page_config(page_title="KB증권)대외계-RMS 분리 작업", layout="wide")
    st.title("🛡️ KB증권)대외계-RMS 분리 작업 점검 시스템")
    
    mapping = load_fep_data()
    
    try:
        all_df = get_all_results() 
    except Exception as e:
        st.error("DB 연결 오류가 발생했습니다. Secrets 설정을 확인하세요.")
        st.stop()
    
    # --- 상단 KPI 대시보드 ---
    if mapping:
        st.markdown("### 📊 진행 현황 요약")
        total_rms, part_rms, total_target, comp_test = get_kpi_metrics(mapping)
        
        rms_prog = (part_rms / total_rms * 100) if total_rms > 0 else 0
        test_prog = (comp_test / total_target * 100) if total_target > 0 else 0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("전체 대상 RMS 부서", f"{total_rms} 개")
        k2.metric("운영반영 확인 RMS", f"{part_rms} 개", f"진행률 {rms_prog:.1f}%")
        k3.metric("전체 대외기관 테스트 대상", f"{total_target} 건")
        k4.metric("테스트 완료 건수", f"{comp_test} 건", f"진척률 {test_prog:.1f}%")
        st.markdown("---")

    # --- 본문 입력 및 조회 ---
    if mapping:
        col1, col2 = st.columns([1, 1.2])
