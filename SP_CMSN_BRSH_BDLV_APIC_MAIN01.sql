create or replace PROCEDURE         "SP_CMSN_BRSH_BDLV_APIC_MAIN01" 
(
    p_bsc_ym              IN VARCHAR2,      -- ���س��
    p_dlv_brsh_cd_fr      IN VARCHAR2,      -- ������� From
    p_dlv_brsh_cd_to      IN VARCHAR2,      -- ������� To
    p_reg_per             IN VARCHAR2,      -- �۾��� ���
    p_pgm_id              IN VARCHAR2       -- ���α׷�ID
) IS
/*################################################################################################*/ 
/* ��      ��  : �������Ȯ��(�����/������ �븮��) ����  Main1 Procedure                         */
/* INPUT PARA : p_bsc_ym :  �۾����                                                              */
/*              p_dlv_brsh_cd_fr :  ������� From                                                 */
/*              p_dlv_brsh_cd_to :  ������� To                                                   */
/*              p_reg_per :  �۾��� ���                                                          */
/*              p_pgm_id :  ���α׷�ID                                                            */
/* ��   ��  �� : JJCH                                                                             */
/* �� ��  �� �� : 2018.01.06                                                                      */
/* ��       �� : �������Ȯ��(�����/������ �븮��) ���������� ���� ���ν��� Log ���� ��          */
/*               �������Ȯ��(�����/������ �븮��) ���ν��� ����(SP_CMSN_BRSH_BDLV_APIC_MAIN01)  */
/*               ���ν����� �����ߴ�. �׷� sql�� ���� git repo�� �ϳ��ϳ� �����ؼ� �����ϰ� �ٽ� Ǫ��? �ϴ� �غ�.  */
/*################################################################################################*/
BEGIN
DECLARE

    v_dlv_brsh_cd         VARCHAR2(5);         -- �������
    v_ymd_fr              VARCHAR2(8);         -- ���������� ���� From
    v_ymd_to              VARCHAR2(8);         -- ���������� ���� To
    v_errdesc             VARCHAR2(1000);      -- ��������
                          
    v_log_tbl_chk         NUMBER(1);           -- log table check
    v_loop_cnt            NUMBER(5);           -- LOOP Count
    v_bsc_cnt             NUMBER(5);           -- ����Ÿ�Ǽ�
    
    USER_ERR              EXCEPTION;           -- ���� ó��
    
    v_err_code            VARCHAR2(3);
    v_err_msg             VARCHAR2(1000);
/*######################################################################################*/
/* ������ ��Ȯ�� ������� ��ȸ                                                          */
/*######################################################################################*/
CURSOR SEL_TDPTMSTR IS
    SELECT BRSH_CD                                AS DLV_BRSH_CD   /* ���� */
         , EXADJ_GRLD_CD                          AS DLV_GRLD_CD   /* ��������ڵ� */
         , COUNT(BRSH_CD) over (PARTITION BY 0)   AS BRSH_CD_CNT   /* ������ ���� Count */
      FROM TMD_BRSH_MMBY_HST
     WHERE BASE_YM = SUBSTR(p_bsc_ym, 1, 6)
       AND CLOS_YMD >= TO_CHAR(ADD_MONTHS(TO_DATE(p_bsc_ym||'01','YYYYMMDD'), -1),'YYYYMMDD')
       AND BRSH_CD BETWEEN p_dlv_brsh_cd_fr AND p_dlv_brsh_cd_to
       AND BRSH_SCT_CD IN('30','50')      /* 30:����, 50:�븮�� */      
  ORDER BY BRSH_CD;
  
/*######################################################################################*/
/* ������ ��Ȯ�� ����Ÿ ��ȸ                                                            */
/*######################################################################################*/
CURSOR SEL_TCADRUNH IS
    SELECT /*+ FULL(A1) */
           A1.INV_NO                                 AS INV_NO    /* ����� */
         , A1.DLV_YMD                                AS DLV_YMD   /* ������� */
      FROM TPS_ACPR_RGST A1                                       /* �μ��ڵ�� */
         , TPS_INV_RGST A2                                        /* ������� */
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
        /* PROGRAM[��ġ���α׷�����α�] ������� START                                     */
        /*  - ���α׷����� ���� �ÿ� SP_CM_JOB_SUBMIT ���ν����� ���ؼ�                     */ 
        /*  - TCM_BPGM_PRGS_LOG ���翩�� üũ�� ���� �� ����                                */
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
               SET PGM_NM = '�������Ȯ��(�����/������ �븮��) ����'
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
                 , '�������Ȯ��(�����/������ �븮��) ����'
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
        /* PROGRAM[�����Ḷ��] üũ                                                         */ 
        /*##################################################################################*/
        v_errdesc  := '';
        v_bsc_cnt  := 0;
        
        /*##################################################################################*/
        /* PROGRAM[������Ը���] üũ                                                       */ 
        /*##################################################################################*/ 
        SELECT COUNT(CLS_YN)
          INTO v_bsc_cnt
          FROM TAP_EXADJ_BUY_CLS                                        /* ������Ը��� */                                                                                                                                                                              
         WHERE EXADJ_YM = p_bsc_ym                                                                                                                                                              
           AND CLS_SCT_CD  = '10'                                       /* 10 : ����� */
           AND NVL(CLS_YN,'N') = 'Y';

        IF (v_bsc_cnt = 0) THEN
            v_errdesc := p_bsc_ym||'�� ����帶���� �ȵǾ����ϴ�.';
            RAISE USER_ERR;
        END IF;   
        
        v_ymd_fr := p_bsc_ym||'01';
        v_ymd_to := p_bsc_ym||'31';
        
        v_loop_cnt := 0;
        
        FOR CUR_DPT IN SEL_TDPTMSTR
        LOOP
            
            /*##############################################################################*/
            /* �������۾�[��ġ���α׷�����α�] ������� START                              */ 
            /*##############################################################################*/
            UPDATE TCM_BPGM_PRGS_LOG 
               SET PRGS_UNT_NM2 = '�������� : ['||CUR_DPT.DLV_BRSH_CD||']'
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
                     /* ��Ȯ���� ����������Ͽ���忡 INSERT(���, ��Ʋ������ �Ѵ� ���ÿ�)  */ 
                     /*#####################################################################*/
                     INSERT 
                       INTO TAP_BDLV_APIC_INV                            /* ����������Ͽ���� */
                     (                                                   
                            INV_NO                                       /* ������ȣ */
                          , CMSN_KND_CD                                  /* �����������ڵ� */
                          , DLVSH_CD                                     /* ������ڵ� */
                          , DLV_YMD                                      /* ������� */
                          , EXADJ_YM                                     /* ������ */
                          , CMSN_APLY_YN                                 /* ���������뿩�� */
                          , CRE_DTM                                      /* �����Ͻ� */ 
                          , CRE_PGM_ID                                   /* ���α׷�ID */ 
                          , CRE_USR_ID                                   /* ���������ID */ 
                          , UPT_DTM                                      /* �����Ͻ� */ 
                          , UPT_PGM_ID                                   /* ���α׷�ID */ 
                          , UPT_USR_ID                                   /* ���������ID */ 
                     )                                                   
                     VALUES                                              
                     (                                                   
                            CUR_NCNF.INV_NO                              
                          , 'C0'                                         /* CO : ��޼����� */
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
                    /* ����ó��                                                            */
                    /*#####################################################################*/
                    EXCEPTION
                        WHEN DUP_VAL_ON_INDEX THEN
                             v_err_code := SQLCODE;
                             v_err_msg  := SQLERRM;
                           
                             v_errdesc := 'SP_CMSN_BRSH_BDLV_APIC_MAIN01 DUP_VAL_ON_INDEX ERROR '
                                          ||'�����:['   ||CUR_NCNF.INV_NO||'] '
                                          ||'�������:[' ||v_dlv_brsh_cd  ||'] '
                                          ||'SQLCODE:['  ||SQLCODE        ||'] '
                                          ||'SQLNAME:['  ||SQLERRM        ||'] ';
                                       
                             /*############################################################*/
                             /* ���� ���� (��������������α�)                             */
                             /*############################################################*/
                             INSERT 
                               INTO TAP_CMSN_EXADJ_MSTK_LOG              /* ��������������α� */
                             (
                                    EXADJ_YM                             /* ������ */
                                  , APP_ID                               /* ���ø����̼�ID */
                                  , MSTK_CD                              /* �����ڵ� */
                                  , MSTK_MSG_CONT                        /* �����޽������� */
                                  , INV_NO                               /* ������ȣ */
                                  , PRCS_VAL                             /* ó���� */
                                  , CRE_DTM                              /* �����Ͻ� */ 
                                  , CRE_PGM_ID                           /* ���α׷�ID */ 
                                  , CRE_USR_ID                           /* ���������ID */ 
                                  , UPT_DTM                              /* �����Ͻ� */ 
                                  , UPT_PGM_ID                           /* ���α׷�ID */ 
                                  , UPT_USR_ID                           /* ���������ID */ 
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
                                          ||'�����:['   ||CUR_NCNF.INV_NO||'] '
                                          ||'�������:[' ||v_dlv_brsh_cd  ||'] '
                                          ||'SQLCODE:['  ||SQLCODE        ||'] '
                                          ||'SQLNAME:['  ||SQLERRM        ||'] ';
                                        
                             /*############################################################*/
                             /* ���� ���� (��������������α�)                             */
                             /*############################################################*/
                             INSERT 
                               INTO TAP_CMSN_EXADJ_MSTK_LOG              /* ��������������α� */ 
                             (
                                    EXADJ_YM                             /* ������ */
                                  , APP_ID                               /* ���ø����̼�ID */
                                  , MSTK_CD                              /* �����ڵ� */
                                  , MSTK_MSG_CONT                        /* �����޽������� */
                                  , INV_NO                               /* ������ȣ */
                                  , PRCS_VAL                             /* ó���� */
                                  , CRE_DTM                              /* �����Ͻ� */ 
                                  , CRE_PGM_ID                           /* ���α׷�ID */ 
                                  , CRE_USR_ID                           /* ���������ID */ 
                                  , UPT_DTM                              /* �����Ͻ� */ 
                                  , UPT_PGM_ID                           /* ���α׷�ID */ 
                                  , UPT_USR_ID                           /* ���������ID */ 
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
            /* �������۾� ������� START                                                    */ 
            /*##############################################################################*/
            UPDATE TCM_BPGM_PRGS_LOG 
               SET WK_END_DTM = TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2')
                 , PRGS_UNT_NM3 = '�������� : ['||CUR_DPT.DLV_BRSH_CD||']'
                 , PRGS_UNT_NM4 = 'COUNT : ['||v_loop_cnt||'/'||CUR_DPT.BRSH_CD_CNT||']'
                 , UPT_USR_ID = p_reg_per
                 , UPT_DTM = SYSDATE
             WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
               AND WK_CMPT_STAT_CD = 'P'
               AND PRGS_UNT_NM1 = p_bsc_ym;
            
            COMMIT;
            /*##############################################################################*/
            /* �������۾� ������� END                                                      */ 
            /* �������� ������ COMMIT                                                       */
            /*##############################################################################*/
            
        END LOOP;    
        
        /*##############################################################################*/
        /* CURSOR �����Ͱ� �������� �������                                            */ 
        /*##############################################################################*/
        IF ( v_loop_cnt = 0 ) THEN       
            v_errdesc := '�������� : ['||p_dlv_brsh_cd_fr||'] �������� : ['||p_dlv_brsh_cd_to||'] �����Ͱ� �������� �ʽ��ϴ�.';
            RAISE USER_ERR;
        END IF;

        /*##################################################################################*/
        /* �������۾�[��ġ���α׷�����α�] ������� END                                    */ 
        /*##################################################################################*/
        UPDATE TCM_BPGM_PRGS_LOG 
           SET WK_CMPT_STAT_CD = 'S'
             , WK_END_DTM = TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF2')
             , RMK = '�������Ȯ��(�����/������ �븮��) ���� ����'
             , UPT_USR_ID = p_reg_per
             , UPT_DTM = SYSDATE
         WHERE PGM_ID = 'SP_CMSN_BRSH_BDLV_APIC_MAIN01'
           AND WK_CMPT_STAT_CD = 'P'
           AND PRGS_UNT_NM1 = p_bsc_ym;
        
        COMMIT;
        /*##################################################################################*/
        /* PROGRAM[��ġ���α׷�����α�] ������� END                                       */ 
        /*##################################################################################*/
                
        /*##################################################################################*/
        /* ����ó��                                                                         */ 
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