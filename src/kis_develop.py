import pandas as pd
import urllib.request
import ssl
import zipfile
import os
import platform


def _dir_seperator_check():
	if platform.system() == 'Windows':
		return '\\'
	else:
		return '/'

def get_domestic_future_master_dataframe(base_dir):

    # download file
    print("Downloading...")

    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/fo_idx_code_mts.mst.zip", base_dir + _dir_seperator_check() + "fo_idx_code_mts.mst.zip")
    os.chdir(base_dir)

    fo_idx_code_zip = zipfile.ZipFile('fo_idx_code_mts.mst.zip')
    fo_idx_code_zip.extractall()
    fo_idx_code_zip.close()
    file_name = base_dir + _dir_seperator_check() + "fo_idx_code_mts.mst"

    columns = ['상품종류','단축코드','표준코드',' 한글종목명',' ATM구분',
               ' 행사가',' 월물구분코드',' 기초자산 단축코드',' 기초자산 명']
    df=pd.read_table(file_name, sep='|',encoding='cp949',header=None)
    df.columns = columns
    df.to_excel('fo_idx_code_mts.xlsx',index=False)  # 현재 위치에 엑셀파일로 저장

    return df

def get_domestic_stk_future_master_dataframe(base_dir):

    # download file
    print("Downloading...")

    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/fo_stk_code_mts.mst.zip", base_dir + _dir_seperator_check() + "fo_stk_code_mts.mst.zip")
    os.chdir(base_dir)

    fo_stk_code_zip = zipfile.ZipFile('fo_stk_code_mts.mst.zip')
    fo_stk_code_zip.extractall()
    fo_stk_code_zip.close()
    file_name = base_dir + _dir_seperator_check() + "fo_stk_code_mts.mst"

    fo_stk_code_zip = zipfile.ZipFile('fo_stk_code_mts.mst.zip')
    fo_stk_code_zip.extractall()
    fo_stk_code_zip.close()
    file_name = base_dir + _dir_seperator_check() + "fo_stk_code_mts.mst"

    columns = ['상품종류','단축코드','표준코드',' 한글종목명',' ATM구분',
               ' 행사가',' 월물구분코드',' 기초자산 단축코드',' 기초자산 명']
    df=pd.read_table(file_name, sep='|',encoding='cp949',header=None)
    df.columns = columns
    df.to_excel('fo_stk_code_mts.xlsx',index=False)  # 현재 위치에 엑셀파일로 저장

    return df

def kosdaq_master_download(base_dir, verbose=False):

    cwd = os.getcwd()
    if (verbose): print(f"current directory is {cwd}")
    ssl._create_default_https_context = ssl._create_unverified_context

    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
                               base_dir + _dir_seperator_check() +  "kosdaq_code.zip")

    os.chdir(base_dir)
    if (verbose): print(f"change directory to {base_dir}")
    kosdaq_zip = zipfile.ZipFile('kosdaq_code.zip')
    kosdaq_zip.extractall()

    kosdaq_zip.close()

    if os.path.exists("kosdaq_code.zip"):
        os.remove("kosdaq_code.zip")

def get_kosdaq_master_dataframe(base_dir):
    file_name = base_dir + _dir_seperator_check() + "kosdaq_code.mst"
    tmp_fil1 = base_dir + _dir_seperator_check() + "kosdaq_code_part1.tmp"
    tmp_fil2 = base_dir + _dir_seperator_check() + "kosdaq_code_part2.tmp"

    wf1 = open(tmp_fil1, mode="w")
    wf2 = open(tmp_fil2, mode="w")

    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0:len(row) - 222]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
            rf2 = row[-222:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    part1_columns = ['단축코드','표준코드','한글종목명']
    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='UTF-8') #CP949로는 제대로 작동되지 않아 수정함

    field_specs = [2, 1,
                   4, 4, 4, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 9,
                   5, 5, 1, 1, 1,
                   2, 1, 1, 1, 2,
                   2, 2, 3, 1, 3,
                   12, 12, 8, 15, 21,
                   2, 7, 1, 1, 1,
                   1, 9, 9, 9, 5,
                   9, 8, 9, 3, 1,
                   1, 1
                   ]

    part2_columns = ['증권그룹구분코드','시가총액 규모 구분 코드 유가',
                     '지수업종 대분류 코드','지수 업종 중분류 코드','지수업종 소분류 코드','벤처기업 여부 (Y/N)',
                     '저유동성종목 여부','KRX 종목 여부','ETP 상품구분코드','KRX100 종목 여부 (Y/N)',
                     'KRX 자동차 여부','KRX 반도체 여부','KRX 바이오 여부','KRX 은행 여부','기업인수목적회사여부',
                     'KRX 에너지 화학 여부','KRX 철강 여부','단기과열종목구분코드','KRX 미디어 통신 여부',
                     'KRX 건설 여부','(코스닥)투자주의환기종목여부','KRX 증권 구분','KRX 선박 구분',
                     'KRX섹터지수 보험여부','KRX섹터지수 운송여부','KOSDAQ150지수여부 (Y,N)','주식 기준가',
                     '정규 시장 매매 수량 단위','시간외 시장 매매 수량 단위','거래정지 여부','정리매매 여부',
                     '관리 종목 여부','시장 경고 구분 코드','시장 경고위험 예고 여부','불성실 공시 여부',
                     '우회 상장 여부','락구분 코드','액면가 변경 구분 코드','증자 구분 코드','증거금 비율',
                     '신용주문 가능 여부','신용기간','전일 거래량','주식 액면가','주식 상장 일자','상장 주수(천)',
                     '자본금','결산 월','공모 가격','우선주 구분 코드','공매도과열종목여부','이상급등종목여부',
                     'KRX300 종목 여부 (Y/N)','매출액','영업이익','경상이익','단기순이익','ROE(자기자본이익률)',
                     '기준년월','전일기준 시가총액 (억)','그룹사 코드','회사신용한도초과여부','담보대출가능여부','대주가능여부'
                     ]

    df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns)

    df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

    # clean temporary file and dataframe
    del (df1)
    del (df2)
    os.remove(tmp_fil1)
    os.remove(tmp_fil2)

    print("Done")

    return df

def kospi_master_download(base_dir, verbose=False):
    cwd = os.getcwd()
    if (verbose): print(f"current directory is {cwd}")
    ssl._create_default_https_context = ssl._create_unverified_context

    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
                               base_dir + _dir_seperator_check() + "kospi_code.zip")

    os.chdir(base_dir)
    if (verbose): print(f"change directory to {base_dir}")
    kospi_zip = zipfile.ZipFile('kospi_code.zip')
    kospi_zip.extractall()

    kospi_zip.close()

    if os.path.exists("kospi_code.zip"):
        os.remove("kospi_code.zip")


def get_kospi_master_dataframe(base_dir):
    file_name = base_dir + _dir_seperator_check() +"kospi_code.mst"
    tmp_fil1 = base_dir + _dir_seperator_check() + "kospi_code_part1.tmp"
    tmp_fil2 = base_dir + _dir_seperator_check() + "kospi_code_part2.tmp"

    wf1 = open(tmp_fil1, mode="w")
    wf2 = open(tmp_fil2, mode="w")

    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0:len(row) - 228]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
            rf2 = row[-228:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    part1_columns = ['단축코드', '표준코드', '한글명']
    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='UTF-8')

    field_specs = [2, 1, 4, 4, 4,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 9, 5, 5, 1,
                   1, 1, 2, 1, 1,
                   1, 2, 2, 2, 3,
                   1, 3, 12, 12, 8,
                   15, 21, 2, 7, 1,
                   1, 1, 1, 1, 9,
                   9, 9, 5, 9, 8,
                   9, 3, 1, 1, 1
                   ]

    part2_columns = ['그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류',
                     '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100',
                     'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100',
                     'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC',
                     'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설',
                     'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송',
                     'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지',
                     '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시',
                     '우회상장', '락구분', '액면변경', '증자구분', '증거금비율',
                     '신용가능', '신용기간', '전일거래량', '액면가', '상장일자',
                     '상장주수', '자본금', '결산월', '공모가', '우선주',
                     '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액',
                     '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월',
                     '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능'
                     ]

    df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns)

    df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

    # clean temporary file and dataframe
    del (df1)
    del (df2)
    os.remove(tmp_fil1)
    os.remove(tmp_fil2)

    print("Done")

    return df

def get_overseas_future_master_dataframe(base_dir):

    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/ffcode.mst.zip", base_dir + _dir_seperator_check() + "ffcode.mst.zip")
    os.chdir(base_dir)
    print("base_dir :" + base_dir)

    nas_zip = zipfile.ZipFile('ffcode.mst.zip')
    nas_zip.extractall()
    nas_zip.close()

    file_name = base_dir + _dir_seperator_check() + "ffcode.mst"
    columns = ['종목코드', '서버자동주문 가능 종목 여부', '서버자동주문 TWAP 가능 종목 여부', '서버자동 경제지표 주문 가능 종목 여부',
               '필러', '종목한글명', '거래소코드 (ISAM KEY 1)', '품목코드 (ISAM KEY 2)', '품목종류', '출력 소수점', '계산 소수점',
               '틱사이즈', '틱가치', '계약크기', '가격표시진법', '환산승수', '최다월물여부 0:원월물 1:최다월물',
               '최근월물여부 0:원월물 1:최근월물', '스프레드여부', '스프레드기준종목 LEG1 여부', '서브 거래소 코드']
    df=pd.DataFrame(columns=columns)
    ridx=1
    print("Downloading...")
    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            a = row[:32]              # 종목코드
            b = row[32:33].rstrip()   # 서버자동주문 가능 종목 여부
            c = row[33:34].rstrip()   # 서버자동주문 TWAP 가능 종목 여부
            d = row[34:35]            # 서버자동 경제지표 주문 가능 종목 여부
            e = row[35:82].rstrip()   # 필러
            f = row[82:132].rstrip()  # 종목한글명
            g = row[132:142]          # 거래소코드 (ISAM KEY 1)
            h = row[142:152].rstrip() # 품목코드 (ISAM KEY 2)
            i = row[152:155].rstrip() # 품목종류
            j = row[155:160]          # 출력 소수점
            k = row[160:165].rstrip() # 계산 소수점
            l = row[165:179].rstrip() # 틱사이즈
            m = row[179:193]          # 틱가치
            n = row[193:203].rstrip() # 계약크기
            o = row[203:207].rstrip() # 가격표시진법
            p = row[207:217]          # 환산승수
            q = row[217:218].rstrip() # 최다월물여부 0:원월물 1:최다월물
            r = row[218:219].rstrip() # 최근월물여부 0:원월물 1:최근월물
            s = row[219:220].rstrip() # 스프레드여부
            t = row[220:221].rstrip() # 스프레드기준종목 LEG1 여부 Y/N
            u = row[221:223].rstrip() # 서브 거래소 코드

            df.loc[ridx] = [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u]
            ridx += 1

    df.to_excel('ffcode.xlsx',index=False)  # 현재 위치에 엑셀파일로 저장

    return df

def get_overseas_index_master_dataframe(base_dir):

    # download file
    print("Downloading...")

    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/frgn_code.mst.zip", base_dir + _dir_seperator_check() +"frgn_code.mst.zip")
    os.chdir(base_dir)

    frgn_code_zip = zipfile.ZipFile('frgn_code.mst.zip')
    frgn_code_zip.extractall()
    frgn_code_zip.close()
    file_name = base_dir + _dir_seperator_check() +"frgn_code.mst"

    # df1 : '구분코드','심볼','영문명','한글명'
    tmp_fil1 = base_dir + _dir_seperator_check() + "frgn_code_part1.tmp"
    tmp_fil2 = base_dir + _dir_seperator_check() + "frgn_code_part2.tmp"
    wf1 = open(tmp_fil1, mode="w")
    wf2 = open(tmp_fil2, mode="w")
    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            if row[0:1] == 'X':
                rf1 = row[0:len(row) - 14]
                rf_1 = rf1[0:1]
                rf1_2 = rf1[1:11]
                rf1_3 = rf1[11:40]
                rf1_4 = rf1[40:80].strip()
                wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + ',' + rf1_4 + '\n')
                rf2 = row[-15:]
                wf2.write(rf2+'\n')
                continue
            rf1 = row[0:len(row) - 14]
            rf1_1 = rf1[0:1]
            rf1_2 = rf1[1:11]
            rf1_3 = rf1[11:50]
            rf1_4 = row[50:75].strip()
            wf1.write(rf1_1 + ',' + rf1_2 + ',' + '"' + rf1_3 + '"' + ',' + '"' +  rf1_4 + '"' + '\n')
            rf2 = row[-15:]
            wf2.write(rf2+'\n')
    wf1.close()
    wf2.close()
    part1_columns = ['구분코드','심볼','영문명','한글명']
    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='UTF-8')

    # df2 : '종목업종코드','다우30 편입종목여부','나스닥100 편입종목여부', 'S&P 500 편입종목여부','거래소코드','국가구분코드'

    field_specs = [4, 1, 1, 1, 4, 3]
    part2_columns = ['종목업종코드','다우30 편입종목여부','나스닥100 편입종목여부',
                     'S&P 500 편입종목여부','거래소코드','국가구분코드']
    df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns)

    df2['종목업종코드'] = df2['종목업종코드'].str.replace(pat=r'[^A-Z]', repl= r'', regex=True) # 종목업종코드는 잘못 기입되어 있을 수 있으니 참고할 때 반드시 mst 파일과 비교 참고
    df2['다우30 편입종목여부'] = df2['다우30 편입종목여부'].str.replace(pat=r'[^0-1]+', repl= r'', regex=True) # 한글명 길이가 길어서 생긴 오타들 제거
    df2['나스닥100 편입종목여부'] = df2['나스닥100 편입종목여부'].str.replace(pat=r'[^0-1]+', repl= r'', regex=True)
    df2['S&P 500 편입종목여부'] = df2['S&P 500 편입종목여부'].str.replace(pat=r'[^0-1]+', repl= r'', regex=True)

    # DF : df1 + df2
    DF = pd.concat([df1,df2],axis=1)
    # print(len(df1), len(df2), len(DF))
    DF.to_excel('frgn_code.xlsx',index=False)  # 현재 위치에 엑셀파일로 저장

    return DF

def get_overseas_master_dataframe(base_dir,val):

    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve(f"https://new.real.download.dws.co.kr/common/master/{val}mst.cod.zip", base_dir + _dir_seperator_check() + f"{val}mst.cod.zip")
    os.chdir(base_dir)

    overseas_zip = zipfile.ZipFile(f'{val}mst.cod.zip')
    overseas_zip.extractall()
    overseas_zip.close()

    file_name = base_dir + _dir_seperator_check() + f"{val}mst.cod"
    columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time(HHMM)', 'market end time(HHMM)', 'DR 여부(Y/N)', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부(0:구성종목없음,1:구성종목있음)', 'Tick size Type', '구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)']

    print(f"Downloading...{val}mst.cod")
    df = pd.read_table(base_dir+_dir_seperator_check() +f"{val}mst.cod",sep='\t',encoding='cp949')
    df.columns = columns
    df.to_excel(f'{val}_code.xlsx',index=False)  # 현재 위치에 엑셀파일로 저장


    return df

#     # 순서대로 나스닥, 뉴욕, 아멕스, 상해, 상해지수, 심천, 심천지수, 도쿄, 홍콩, 하노이, 호치민
#     lst = ['nas','nys','ams','shs','shi','szs','szi','tse','hks','hnx','hsx']
#     각각의 데이터를 받아 쓸 수 있음.

def get_sector_master_dataframe(base_dir):

    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/idxcode.mst.zip", base_dir + _dir_seperator_check() + "idxcode.zip")
    os.chdir(base_dir)

    idxcode_zip = zipfile.ZipFile('idxcode.zip')
    idxcode_zip.extractall()
    idxcode_zip.close()

    file_name = base_dir + _dir_seperator_check() + "idxcode.mst"
    df = pd.DataFrame(columns = ['업종코드', '업종명'])

    ridx = 1
    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            tcode = row[1:5]  # 업종코드 4자리 (맨 앞 1자리 제거)
            tname = row[3:43].rstrip() #업종명
            df.loc[ridx] = [tcode, tname]
            # print(df.loc[ridx])  # 파일 작성중인 것을 확인할 수 있음
            ridx += 1

    return df

def get_theme_master_dataframe(base_dir):

    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/theme_code.mst.zip", base_dir + _dir_seperator_check() + "theme_code.zip")
    os.chdir(base_dir)

    kospi_zip = zipfile.ZipFile('theme_code.zip')
    kospi_zip.extractall()
    kospi_zip.close()

    file_name = base_dir + _dir_seperator_check() + "theme_code.mst"
    df = pd.DataFrame(columns = ['테마코드', '테마명', '종목코드'])

    ridx = 1
    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            tcode = row[0:3]            # 테마코드
            jcode = row[-10:].rstrip()  # 테마명
            tname = row[3:-10].rstrip() # 종목코드
            df.loc[ridx] = [tcode, tname, jcode]
            # print(df.loc[ridx])  # 파일 작성중인 것을 확인할 수 있음
            ridx += 1

    return df
