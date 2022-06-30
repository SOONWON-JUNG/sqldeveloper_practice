create or replace PROCEDURE         "SP_CMSN_BRSH_BDLV_APIC_MAIN01" 
(
    p_bsc_ym              IN VARCHAR2,      -- 기준년월
    p_dlv_brsh_cd_fr      IN VARCHAR2,      -- 배달점소 From
    p_dlv_brsh_cd_to      IN VARCHAR2,      -- 배달점소 To
    p_reg_per             IN VARCHAR2,      -- 작업자 사번
    p_pgm_id              IN VARCHAR2       -- 프로그램ID
) IS
/*################################################################################################*/ 
/* 설      명  : 수수료미확정(선배달/후집하 대리점) 실행  Main1 Procedure                         */
/* INPUT PARA : p_bsc_ym :  작업년월                                                              */
/*              p_dlv_brsh_cd_fr :  배달점소 From                                                 */
/*              p_dlv_brsh_cd_to :  배달점소 To                                                   */
/*              p_reg_per :  작업자 사번                                                          */
/*              p_pgm_id :  프로그램ID                                                            */
/* 개   발  자 : JJCH                                                                             */
/* 개 발  일 자 : 2018.01.06                                                                      */
/* 참       고 : 수수료미확정(선배달/후집하 대리점) 수수료정산 생성 프로시져 Log 관리 및          */
/*               수수료미확정(선배달/후집하 대리점) 프로시져 실행(SP_CMSN_BRSH_BDLV_APIC_MAIN01)  */
/*               프로시져를 수정했다. 그럼 sql로 빼서 git repo에 하나하나 지정해서 저장하고 다시 푸시? 일단 해봄.  */
/*################################################################################################*/
BEGIN
DECLARE

    v_dlv_brsh_cd         VARCHAR2(5);         -- 배달점소
    v_ymd_fr              VARCHAR2(8);         -- 수수료정산 일자 From
    v_ymd_to              VARCHAR2(8);         -- 수수료정산 일자 To
    v_errdesc             VARCHAR2(1000);      -- 에러설명
                          
    v_log_tbl_chk         NUMBER(1);           -- log table check
    v_loop_cnt            NUMBER(5);           -- LOOP Count
    v_bsc_cnt             NUMBER(5);           -- 데이타건수
    
    USER_ERR              EXCEPTION;           -- 예외 처리
    
    v_err_code            VARCHAR2(3);
    v_err_msg             VARCHAR2(1000);
/*######################################################################################*/
/* 수수료 미확정 대상점소 조회                                                          */
/*######################################################################################*/
CURSOR SEL_TDPTMSTR IS
    SELECT BRSH_CD                                AS DLV_BRSH_CD   /* 점소 */
         , EXADJ_GRLD_CD                          AS DLV_GRLD_CD   /* 정산급지코드 */
         , COUNT(BRSH_CD) over (PARTITION BY 0)   AS BRSH_CD_CNT   /* 정산대상 점소 Count */
      FROM TMD_BRSH_MMBY_HST
     WHERE BASE_YM = SUBSTR(p_bsc_ym, 1, 6)
       AND CLOS_YMD >= TO_CHAR(ADD_MONTHS(TO_DATE(p_bsc_ym||'01','YYYYMMDD'), -1),'YYYYMMDD')
       AND BRSH_CD BETWEEN p_dlv_brsh_cd_fr AND p_dlv_brsh_cd_to
       AND BRSH_SCT_CD IN('30','50')      /* 30:지점, 50:대리점 */      
  ORDER BY BRSH_CD;
  
/*######################################################################################*/
/* 수수료 미확정 데이타 조회                                                            */
/*######################################################################################*/
CURSOR SEL_TCADRUNH IS
    SELECT /*+ FULL(A1) */
           A1.INV_NO                                 AS INV_NO    /* 운송장 */
         , A1.DLV_YMD                                AS DLV_YMD   /* 배달일자 */
      FROM TPS_ACPR_RGST A1                                       /* 인수자등록 */
         , TPS_INV_RGST A2                                        /* 운송장등록 */
     WHERE A1.INV_NO = A2.INV_NO(+)
       AND A1.DLVSH_CD = v_dlv_brsh_cd
       AND A1.DLV_YMD >= v_ymd_fr
       AND A1.DLV_YMD <= v_ymd_to
       AND (
            A2.PICK_YMD IS NULL 
            OR A2.PICK_YMD > v_ymd_to
           );

    BEGIN

        /*##################################################################################*/
        /* PROGRAM[배치프로그램진행로그] 진행상태 START                                     */
        /*  - 프로그램에서 실행 시에 SP_CM_JOB_SUBMIT 프로시져를 통해서                     */ 
        /*  - TCM_BPGM_PRGS_LOG 존재여부 체크후 저장 및 수정                                */
        /*##################################################################################*/
        v_log_tbl_chk := 0;

        SELECT COUNT(PGM_ID)
          INTO v_log_tbl_chk
          FROM TCM_BPGM_PRGS_LOG
         WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
           AND WK_CMPT_STAT_CD = 'P'
           AND PRGS_UNT_NM1 = p_bsc_ym;

        IF v_log_tbl_chk > 0 THEN
            UPDATE TCM_BPGM_PRGS_LOG 
               SET PGM_NM = '수수료미확정(선배달/후집하 대리점) 정산'
                 , PRGS_UNT_NM10 = 'FROM: ['||p_dlv_brsh_cd_fr||']'
                 , PRGS_UNT_NM11 = 'TO: ['||p_dlv_brsh_cd_to||']'
                 , UPT_USR_ID = p_reg_per
                 , UPT_DTM = SYSDATE
             WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
               AND WK_CMPT_STAT_CD = 'P'
               AND PRGS_UNT_NM1 = p_bsc_ym;
              
            COMMIT;
        ELSE        
            INSERT 
              INTO TCM_BPGM_PRGS_LOG 
            (
                   PGM_ID
                 , WK_STRT_DTM
                 , PGM_NM
                 , WK_CMPT_STAT_CD
                 , PRGS_UNT_NM1
                 , PRGS_UNT_NM10
                 , PRGS_UNT_NM11
                 , CRE_USR_ID
                 , CRE_PGM_ID
                 , CRE_DTM
                 , UPT_USR_ID
                 , UPT_PGM_ID
                 , UPT_DTM
            )
            VALUES 
            (
                   'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
                 , TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2')
                 , '수수료미확정(선배달/후집하 대리점) 정산'
                 , 'P'
                 , p_bsc_ym
                 , 'FROM : ['||p_dlv_brsh_cd_fr||']'
                 , 'TO : ['||p_dlv_brsh_cd_to||']'
                 , p_reg_per
                 , p_pgm_id
                 , SYSDATE
                 , p_reg_per
                 , p_pgm_id
                 , SYSDATE
            );
            COMMIT;
        END IF;
        
        
        /*##################################################################################*/
        /* PROGRAM[수수료마감] 체크                                                         */ 
        /*##################################################################################*/
        v_errdesc  := '';
        v_bsc_cnt  := 0;
        
        /*##################################################################################*/
        /* PROGRAM[정산매입마감] 체크                                                       */ 
        /*##################################################################################*/ 
        SELECT COUNT(CLS_YN)
          INTO v_bsc_cnt
          FROM TAP_EXADJ_BUY_CLS                                        /* 정산매입마감 */                                                                                                                                                                              
         WHERE EXADJ_YM = p_bsc_ym                                                                                                                                                              
           AND CLS_SCT_CD  = '10'                                       /* 10 : 운송장 */
           AND NVL(CLS_YN,'N') = 'Y';

        IF (v_bsc_cnt = 0) THEN
            v_errdesc := p_bsc_ym||'월 운송장마감이 안되었습니다.';
            RAISE USER_ERR;
        END IF;   
        
        v_ymd_fr := p_bsc_ym||'01';
        v_ymd_to := p_bsc_ym||'31';
        
        v_loop_cnt := 0;
        
        FOR CUR_DPT IN SEL_TDPTMSTR
        LOOP
            
            /*##############################################################################*/
            /* 수수료작업[배치프로그램진행로그] 진행상태 START                              */ 
            /*##############################################################################*/
            UPDATE TCM_BPGM_PRGS_LOG 
               SET PRGS_UNT_NM2 = '시작점소 : ['||CUR_DPT.DLV_BRSH_CD||']'
                 , UPT_USR_ID = p_reg_per
                 , UPT_DTM = SYSDATE
             WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
               AND WK_CMPT_STAT_CD = 'P'
               AND PRGS_UNT_NM1 = p_bsc_ym;
               
            COMMIT;
            
            v_dlv_brsh_cd := CUR_DPT.DLV_BRSH_CD;
            
            FOR CUR_NCNF IN SEL_TCADRUNH
            LOOP
                BEGIN                     
                     /*#####################################################################*/
                     /* 미확정건 선배달후집하운송장에 INSERT(배송, 셔틀수수료 둘다 동시에)  */ 
                     /*#####################################################################*/
                     INSERT 
                       INTO TAP_BDLV_APIC_INV                            /* 선배달후집하운송장 */
                     (                                                   
                            INV_NO                                       /* 운송장번호 */
                          , CMSN_KND_CD                                  /* 수수료종류코드 */
                          , DLVSH_CD                                     /* 배달점코드 */
                          , DLV_YMD                                      /* 배달일자 */
                          , EXADJ_YM                                     /* 정산년월 */
                          , CMSN_APLY_YN                                 /* 수수료적용여부 */
                          , CRE_DTM                                      /* 생성일시 */ 
                          , CRE_PGM_ID                                   /* 프로그램ID */ 
                          , CRE_USR_ID                                   /* 생성사용자ID */ 
                          , UPT_DTM                                      /* 수정일시 */ 
                          , UPT_PGM_ID                                   /* 프로그램ID */ 
                          , UPT_USR_ID                                   /* 수정사용자ID */ 
                     )                                                   
                     VALUES                                              
                     (                                                   
                            CUR_NCNF.INV_NO                              
                          , 'C0'                                         /* CO : 배달수수료 */
                          , v_dlv_brsh_cd
                          , CUR_NCNF.DLV_YMD
                          , 'XXXXXX'
                          , 'N'
                          , SYSDATE
                          , p_pgm_id
                          , p_reg_per
                          , SYSDATE
                          , p_pgm_id
                          , p_reg_per
                     );
                     
                     COMMIT;

                    /*#####################################################################*/
                    /* 예외처리                                                            */
                    /*#####################################################################*/
                    EXCEPTION
                        WHEN DUP_VAL_ON_INDEX THEN
                             v_err_code := SQLCODE;
                             v_err_msg  := SQLERRM;
                           
                             v_errdesc := 'SP_CMSN_BRSH_BDLV_APIC_MAIN01 DUP_VAL_ON_INDEX ERROR '
                                          ||'운송장:['   ||CUR_NCNF.INV_NO||'] '
                                          ||'배달점소:[' ||v_dlv_brsh_cd  ||'] '
                                          ||'SQLCODE:['  ||SQLCODE        ||'] '
                                          ||'SQLNAME:['  ||SQLERRM        ||'] ';
                                       
                             /*############################################################*/
                             /* 예외 에러 (수수료정산오류로그)                             */
                             /*############################################################*/
                             INSERT 
                               INTO TAP_CMSN_EXADJ_MSTK_LOG              /* 수수료정산오류로그 */
                             (
                                    EXADJ_YM                             /* 정산년월 */
                                  , APP_ID                               /* 애플리케이션ID */
                                  , MSTK_CD                              /* 오류코드 */
                                  , MSTK_MSG_CONT                        /* 오류메시지내용 */
                                  , INV_NO                               /* 운송장번호 */
                                  , PRCS_VAL                             /* 처리값 */
                                  , CRE_DTM                              /* 생성일시 */ 
                                  , CRE_PGM_ID                           /* 프로그램ID */ 
                                  , CRE_USR_ID                           /* 생성사용자ID */ 
                                  , UPT_DTM                              /* 수정일시 */ 
                                  , UPT_PGM_ID                           /* 프로그램ID */ 
                                  , UPT_USR_ID                           /* 수정사용자ID */ 
                             )
                             VALUES 
                             (
                                    p_bsc_ym
                                  , 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
                                  , v_err_code
                                  , v_err_msg
                                  , CUR_NCNF.INV_NO
                                  , v_errdesc
                                  , SYSDATE
                                  , p_pgm_id
                                  , p_reg_per
                                  , SYSDATE
                                  , p_pgm_id
                                  , p_reg_per
                             );
                             
                             COMMIT;

                        WHEN OTHERS THEN
                             v_err_code := SQLCODE;
                             v_err_msg  := SQLERRM;
                             
                             v_errdesc := 'SP_CMSN_BRSH_BDLV_APIC_MAIN01 OTHERS ERROR '
                                          ||'운송장:['   ||CUR_NCNF.INV_NO||'] '
                                          ||'배달점소:[' ||v_dlv_brsh_cd  ||'] '
                                          ||'SQLCODE:['  ||SQLCODE        ||'] '
                                          ||'SQLNAME:['  ||SQLERRM        ||'] ';
                                        
                             /*############################################################*/
                             /* 예외 에러 (수수료정산오류로그)                             */
                             /*############################################################*/
                             INSERT 
                               INTO TAP_CMSN_EXADJ_MSTK_LOG              /* 수수료정산오류로그 */ 
                             (
                                    EXADJ_YM                             /* 정산년월 */
                                  , APP_ID                               /* 애플리케이션ID */
                                  , MSTK_CD                              /* 오류코드 */
                                  , MSTK_MSG_CONT                        /* 오류메시지내용 */
                                  , INV_NO                               /* 운송장번호 */
                                  , PRCS_VAL                             /* 처리값 */
                                  , CRE_DTM                              /* 생성일시 */ 
                                  , CRE_PGM_ID                           /* 프로그램ID */ 
                                  , CRE_USR_ID                           /* 생성사용자ID */ 
                                  , UPT_DTM                              /* 수정일시 */ 
                                  , UPT_PGM_ID                           /* 프로그램ID */ 
                                  , UPT_USR_ID                           /* 수정사용자ID */ 
                             )
                             VALUES 
                             (
                                    p_bsc_ym
                                  , 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
                                  , v_err_code
                                  , v_err_msg
                                  , CUR_NCNF.INV_NO
                                  , v_errdesc
                                  , SYSDATE
                                  , p_pgm_id
                                  , p_reg_per
                                  , SYSDATE
                                  , p_pgm_id
                                  , p_reg_per
                             );
                             
                             COMMIT;
                END;
            END LOOP;

            v_loop_cnt := v_loop_cnt + 1;
      
            /*##############################################################################*/
            /* 수수료작업 진행상태 START                                                    */ 
            /*##############################################################################*/
            UPDATE TCM_BPGM_PRGS_LOG 
               SET WK_END_DTM = TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2')
                 , PRGS_UNT_NM3 = '종료점소 : ['||CUR_DPT.DLV_BRSH_CD||']'
                 , PRGS_UNT_NM4 = 'COUNT : ['||v_loop_cnt||'/'||CUR_DPT.BRSH_CD_CNT||']'
                 , UPT_USR_ID = p_reg_per
                 , UPT_DTM = SYSDATE
             WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
               AND WK_CMPT_STAT_CD = 'P'
               AND PRGS_UNT_NM1 = p_bsc_ym;
            
            COMMIT;
            /*##############################################################################*/
            /* 수수료작업 진행상태 END                                                      */ 
            /* 집하점소 단위로 COMMIT                                                       */
            /*##############################################################################*/
            
        END LOOP;    
        
        /*##############################################################################*/
        /* CURSOR 데이터가 존재하지 않을경우                                            */ 
        /*##############################################################################*/
        IF ( v_loop_cnt = 0 ) THEN       
            v_errdesc := '시작점소 : ['||p_dlv_brsh_cd_fr||'] 종료점소 : ['||p_dlv_brsh_cd_to||'] 데이터가 존재하지 않습니다.';
            RAISE USER_ERR;
        END IF;

        /*##################################################################################*/
        /* 수수료작업[배치프로그램진행로그] 진행상태 END                                    */ 
        /*##################################################################################*/
        UPDATE TCM_BPGM_PRGS_LOG 
           SET WK_CMPT_STAT_CD = 'S'
             , WK_END_DTM = TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2')
             , RMK = '수수료미확정(선배달/후집하 대리점) 정산 종료'
             , UPT_USR_ID = p_reg_per
             , UPT_DTM = SYSDATE
         WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
           AND WK_CMPT_STAT_CD = 'P'
           AND PRGS_UNT_NM1 = p_bsc_ym;
        
        COMMIT;
        /*##################################################################################*/
        /* PROGRAM[배치프로그램진행로그] 진행상태 END                                       */ 
        /*##################################################################################*/
                
        /*##################################################################################*/
        /* 예외처리                                                                         */ 
        /*##################################################################################*/    
        EXCEPTION
            WHEN USER_ERR THEN
                 ROLLBACK;
                
                 UPDATE TCM_BPGM_PRGS_LOG 
                    SET WK_CMPT_STAT_CD = 'E'
                      , RMK = v_errdesc
                      , UPT_USR_ID = p_reg_per
                      , UPT_DTM = SYSDATE
                  WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
                    AND WK_CMPT_STAT_CD = 'P'
                    AND PRGS_UNT_NM1 = p_bsc_ym;

                 COMMIT;
                 
            WHEN NO_DATA_FOUND THEN
                 ROLLBACK;
                 v_errdesc := '['||v_dlv_brsh_cd||'] '
                              ||'SP_CMSN_BRSH_BDLV_APIC_MAIN01 NO_DATA_FOUND ERROR'
                              ||'SQLCODE:['||SQLCODE||'] '
                              ||'SQLNAME:['||SQLERRM||'] ';
                 
                 UPDATE TCM_BPGM_PRGS_LOG 
                    SET WK_CMPT_STAT_CD = 'E'
                      , RMK = v_errdesc
                      , UPT_USR_ID = p_reg_per
                      , UPT_DTM = SYSDATE
                  WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
                    AND WK_CMPT_STAT_CD = 'P'
                    AND PRGS_UNT_NM1 = p_bsc_ym;

                 COMMIT;
                 
            WHEN DUP_VAL_ON_INDEX THEN
                 ROLLBACK;
                 v_errdesc := '['||v_dlv_brsh_cd||'] '
                              ||'SP_CMSN_BRSH_BDLV_APIC_MAIN01 DUP_VAL_ON_INDEX ERROR'
                              ||'SQLCODE:['||SQLCODE||'] '
                              ||'SQLNAME:['||SQLERRM||'] ';
                 
                 UPDATE TCM_BPGM_PRGS_LOG 
                    SET WK_CMPT_STAT_CD = 'E'
                      , RMK = v_errdesc
                      , UPT_USR_ID = p_reg_per
                      , UPT_DTM = SYSDATE
                  WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
                    AND WK_CMPT_STAT_CD = 'P'
                    AND PRGS_UNT_NM1 = p_bsc_ym;

                 COMMIT;
           
            WHEN OTHERS THEN
                 ROLLBACK;
                 v_errdesc := '['||v_dlv_brsh_cd||'] '
                             ||'SP_CMSN_BRSH_BDLV_APIC_MAIN01 OTHERS ERROR'
                             ||'SQLCODE:['||SQLCODE||'] '
                             ||'SQLNAME:['||SQLERRM||'] ';
                            
                 UPDATE TCM_BPGM_PRGS_LOG 
                    SET WK_CMPT_STAT_CD = 'E'
                      , RMK = v_errdesc
                      , UPT_USR_ID = p_reg_per
                      , UPT_DTM = SYSDATE
                  WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
                    AND WK_CMPT_STAT_CD = 'P'
                    AND PRGS_UNT_NM1 = p_bsc_ym;

                 COMMIT;
    END;

END SP_CMSN_BRSH_BDLV_APIC_MAIN01;