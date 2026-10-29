#Import requirements
import os
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

st.set_page_config(page_title="Bảng điều khiển cổ phiếu", page_icon="📈", layout="wide")

#Snowflake Connection (credentials are read from .env at the project root)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "STOCK_MDS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "COMMON"),
    )

def run_query(sql):
    cur = get_connection().cursor()
    try:
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()

#Gold tables
@st.cache_data(ttl=30)
def load_tong_quan():
    return run_query("SELECT * FROM tong_quan_co_phieu ORDER BY ma_co_phieu")

@st.cache_data(ttl=30)
def load_nen_gia():
    return run_query("SELECT * FROM fct_candlesticks ORDER BY ma_co_phieu, thoi_gian_nen")

@st.cache_data(ttl=30)
def load_treemap():
    return run_query("SELECT * FROM fct_treemaps")

#Sidebar
st.sidebar.header("Cài đặt")
chu_ky = st.sidebar.select_slider("Tự làm mới (giây)", options=[10, 30, 60, 120], value=30)
if st.sidebar.button("Làm mới ngay"):
    st.cache_data.clear()

st.title("📈 Bảng điều khiển cổ phiếu thời gian thực")

@st.fragment(run_every=chu_ky)
def dashboard():
    try:
        tong_quan = load_tong_quan()
        nen_gia = load_nen_gia()
        treemap = load_treemap()
    except Exception as e:
        st.error(f"Không kết nối được Snowflake: {e}")
        return

    st.caption(f"Cập nhật lúc {pd.Timestamp.now():%H:%M:%S %d/%m/%Y}")

    if tong_quan.empty:
        st.info("Chưa có dữ liệu trong tầng gold. Hãy chạy producer, consumer và DAG "
                "`minio_to_snowflake` để nạp dữ liệu vào Snowflake.")
        return

    #KPI: latest quote per symbol
    st.subheader("Tổng quan")
    for cot, dong in zip(st.columns(len(tong_quan)), tong_quan.itertuples()):
        cot.metric(
            dong.ma_co_phieu,
            f"${dong.gia_hien_tai:,.2f}",
            f"{dong.muc_thay_doi:+.2f} ({dong.phan_tram_thay_doi:+.2f}%)",
        )

    trai, phai = st.columns([3, 2])

    #Candlestick: daily candles + average price line
    with trai:
        st.subheader("Biểu đồ nến")
        ma_chon = st.selectbox("Mã cổ phiếu", sorted(nen_gia["ma_co_phieu"].unique()))
        nen = nen_gia[nen_gia["ma_co_phieu"] == ma_chon]
        fig = go.Figure([
            go.Candlestick(
                x=nen["thoi_gian_nen"], open=nen["gia_mo_cua"], high=nen["gia_cao_nhat"],
                low=nen["gia_thap_nhat"], close=nen["gia_dong_cua"], name="Giá",
            ),
            go.Scatter(
                x=nen["thoi_gian_nen"], y=nen["duong_xu_huong"], name="Giá trung bình ngày",
                mode="lines", line=dict(width=1.5),
            ),
        ])
        fig.update_layout(xaxis_rangeslider_visible=False, height=450,
                          margin=dict(l=10, r=10, t=10, b=10), yaxis_title="USD")
        st.plotly_chart(fig, width="stretch")

        #Candle table for the selected symbol, newest session first, OHLC order
        with st.expander(f"Dữ liệu nến – {ma_chon}"):
            bang = nen[["thoi_gian_nen", "gia_mo_cua", "gia_cao_nhat", "gia_thap_nhat",
                        "gia_dong_cua", "duong_xu_huong"]].astype({"gia_mo_cua": float, "gia_dong_cua": float})
            bang = bang.assign(
                phan_tram=(bang["gia_dong_cua"] - bang["gia_mo_cua"]) / bang["gia_mo_cua"] * 100
            ).sort_values("thoi_gian_nen", ascending=False)
            mau = lambda v: f"color: {'#2e7d32' if v > 0 else '#d32f2f' if v < 0 else 'inherit'}"
            gia = st.column_config.NumberColumn
            st.dataframe(
                bang.style.map(mau, subset=["phan_tram"]),
                width="stretch", hide_index=True,
                column_config={
                    "thoi_gian_nen": st.column_config.DateColumn("Ngày", format="DD/MM/YYYY"),
                    "gia_mo_cua": gia("Mở cửa", format="$%.2f"),
                    "gia_cao_nhat": gia("Cao nhất", format="$%.2f"),
                    "gia_thap_nhat": gia("Thấp nhất", format="$%.2f"),
                    "gia_dong_cua": gia("Đóng cửa", format="$%.2f"),
                    "duong_xu_huong": gia("Giá TB ngày", format="$%.2f"),
                    "phan_tram": gia("% trong phiên", format="%+.2f%%",
                                     help="(Đóng cửa − Mở cửa) / Mở cửa"),
                },
            )

    #Treemap: size = current price, color = % change vs previous session close
    with phai:
        st.subheader("Treemap tăng/giảm so với phiên trước")
        if treemap.empty:
            st.info("Chưa có dữ liệu treemap.")
        else:
            pct = treemap["phan_tram_thay_doi"].astype(float)
            # symmetric scale around 0, at least ±3% so small moves stay pale
            bien = max(3.0, pct.abs().max())
            # format text in Python so every tile shows 2 decimals and an explicit sign
            nhan = [
                f"<b>{ma}</b><br>${gia:,.2f}<br>{doi:+.2f} ({p:+.2f}%)"
                for ma, gia, doi, p in zip(treemap["ma_co_phieu"], treemap["gia_hien_tai"].astype(float),
                                           treemap["muc_thay_doi"].astype(float), pct)
            ]
            fig = go.Figure(go.Treemap(
                labels=treemap["ma_co_phieu"], parents=[""] * len(treemap),
                values=treemap["gia_hien_tai"], text=nhan,
                texttemplate="%{text}", hovertemplate="%{text}<extra></extra>",
                marker=dict(
                    colors=pct, cmin=-bien, cmax=bien, showscale=True,
                    colorscale=["#d32f2f", "#eeeeee", "#2e7d32"],
                    colorbar=dict(title="% so với<br>phiên trước", ticksuffix="%"),
                ),
            ))
            fig.update_layout(height=450, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, width="stretch")

dashboard()
