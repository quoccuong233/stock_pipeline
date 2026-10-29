-- Treemap: kích thước ô = giá hiện tại, màu ô = % thay đổi so với giá đóng cửa phiên trước
select
    ma_co_phieu,
    gia_hien_tai,
    muc_thay_doi,
    phan_tram_thay_doi
from {{ ref('int_data') }}
-- lấy báo giá mới nhất của từng mã
qualify row_number() over (partition by ma_co_phieu order by thoi_gian_lay_du_lieu desc) = 1
order by ma_co_phieu
