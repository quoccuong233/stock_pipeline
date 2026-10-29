SELECT
    ma_co_phieu,
    gia_hien_tai,
    gia_cao_nhat_ngay,
    gia_thap_nhat_ngay,
    gia_mo_cua_ngay,
    gia_dong_cua_truoc,
    muc_thay_doi,
    phan_tram_thay_doi,
    thoi_gian_thi_truong,
    thoi_gian_lay_du_lieu
FROM {{ ref('stg_data') }}
-- Finnhub trả về giá 0 khi mã không hợp lệ hoặc không có dữ liệu
WHERE gia_hien_tai > 0
-- Ngoài giờ giao dịch API trả về cùng một báo giá nhiều lần: chỉ giữ bản lấy gần nhất
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ma_co_phieu, thoi_gian_thi_truong
    ORDER BY thoi_gian_lay_du_lieu DESC
) = 1
