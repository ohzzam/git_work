<template>
<div id="test">

    <div id="top">
        <el-button type="text" @click="search" class="button">조 회</el-button>
        <el-button type="text" @click="handleadd" class="button">추 가</el-button>
    </div>

    <div style="margin-top:15px">

        <el-table
                ref="testTable"
                :data="tableData"
                style="width:100%"
                border
        >
            <el-table-column
                    prop="job_NAME"
                    label="작업명"
                    sortable
                    show-overflow-tooltip>
            </el-table-column>

            <el-table-column
                    prop="job_GROUP"
                    label="작업그룹"
                    sortable>
            </el-table-column>

            <el-table-column
                    prop="job_CLASS_NAME"
                    label="작업클래스명"
                    sortable>
            </el-table-column>

            <el-table-column
                    prop="trigger_NAME"
                    label="트리거명"
                    sortable>
            </el-table-column>

            <!--<el-table-column
                    prop="trigger_GROUP"
                    label="트리거그룹"
                    sortable>
            </el-table-column>-->

            <el-table-column
                    prop="cron_EXPRESSION"
                    label="표현식"
                    sortable>
            </el-table-column>

            <!--<el-table-column
                    prop="time_ZONE_ID"
                    label="타임존"
                    sortable>
            </el-table-column>-->
            
            <el-table-column
                    prop="last_FIRE_TIME"
                    label="마지막실행"
                    :formatter="dateFormat"
                    sortable>
            </el-table-column>
            
            <el-table-column
                    prop="last_SUCCESS_TIME"
                    label="마지막성공"
                    :formatter="dateFormat"
                    sortable>
            </el-table-column>

            <el-table-column label="변경" width="300">
                <template scope="scope">
                    <el-button
                            size="small"
                            type="warning"
                            @click="handlePause(scope.$index, scope.row)">중지</el-button>

                    <el-button
                            size="small"
                            type="info"
                            @click="handleResume(scope.$index, scope.row)">재시작</el-button>

                    <el-button
                            size="small"
                            type="danger"
                            @click="handleDelete(scope.$index, scope.row)">삭제</el-button>

                    <el-button
                            size="small"
                            type="success"
                            @click="handleUpdate(scope.$index, scope.row)">수정</el-button>
                </template>
            </el-table-column>
        </el-table>

        <div align="center">
            <el-pagination
                    @size-change="handleSizeChange"
                    @current-change="handleCurrentChange"
                    :current-page="currentPage"
                    :page-sizes="[10, 20, 30, 40]"
                    :page-size="pagesize"
                    layout="total, sizes, prev, pager, next, jumper"
                    :total="totalCount">
            </el-pagination>
        </div>
    </div>
    <el-dialog title="작업선택" :visible.sync="checkboxChange">
            <el-radio-group v-model="ruleForm.resource">
                <el-radio :label="3">Simple Trigger</el-radio>
                <el-radio :label="6">Cron Trigger</el-radio>
            </el-radio-group>
        <div slot="footer" class="dialog-footer">
            <el-button @click="checkboxChange = false">취소</el-button>
            <el-button type="primary" @click="change">확인</el-button>
        </div>
    </el-dialog>
    <el-dialog title="작업추가" :visible.sync="dialogFormVisibleChange" v-if="ruleForm.resource==3">
        <el-form :model="form" >
            <el-form-item label="작업명" label-width="150px" style="width:35%">
                <!--<el-input v-model="form.jobName" auto-complete="off"></el-input>-->
                <el-select v-model="form.jobName"  placeholder="선택">
                    <el-option
                            v-for="item in jobs"
                            :key="item.value"
                            :label="item.label"
                            :value="item.value">
                    </el-option>
                </el-select>
            </el-form-item>
            <!--<el-form-item label="작업그룹" label-width="150px" style="width:35%">
                <el-input v-model="form.jobGroup" auto-complete="off"></el-input>
            </el-form-item>-->
            <el-form-item label="표현식" label-width="150px" style="width:35%">
                <el-input v-model="form.cronExpression" auto-complete="off"></el-input>

                <el-select v-model="value4"  placeholder="선택">
                    <el-option
                            v-for="item in options"
                            :key="item.value"
                            :label="item.label"
                            :value="item.value">
                    </el-option>
                </el-select>
            </el-form-item>
        </el-form>
        <div slot="footer" class="dialog-footer">
            <el-button @click="dialogFormVisibleChange = false">취소</el-button>
            <el-button type="primary" @click="addSimTir">확인</el-button>
        </div>
    </el-dialog>
    <el-dialog title="작업추가" :visible.sync="dialogFormVisibleChange" v-if="ruleForm.resource==6">
        <el-form :model="form">
            <el-form-item label="작업명" label-width="150px" style="width:35%">
                <!--<el-input v-model="form.jobName" auto-complete="off"></el-input>-->
                <el-select v-model="form.jobName"  placeholder="선택">
                    <el-option
                            v-for="item in jobs"
                            :key="item.value"
                            :label="item.label"
                            :value="item.value">
                    </el-option>
                </el-select>
            </el-form-item>
            <!--<el-form-item label="작업그룹" label-width="150px" style="width:35%">
                <el-input v-model="form.jobGroup" auto-complete="off"></el-input>
            </el-form-item>-->
            <el-form-item label="표현식" label-width="150px" style="width:35%">
                <el-input v-model="form.cronExpression" auto-complete="off"></el-input>
            </el-form-item>
        </el-form>
        <div slot="footer" class="dialog-footer">
            <el-button @click="dialogFormVisibleChange = false">취소</el-button>
            <el-button type="primary" @click="add">확인</el-button>
        </div>
    </el-dialog>

    <el-dialog title="작업수정" :visible.sync="updateFormVisible">
        <el-form :model="updateform">
            <el-form-item label="표현식" label-width="150px" style="width:35%">
                <el-input v-model="updateform.cronExpression" auto-complete="off"></el-input>
            </el-form-item>
            <el-form-item label="마지막성공시간" label-width="150px" style="width:35%">
                <el-input v-model="updateform.lastSuccessTime" auto-complete="off"></el-input>
            </el-form-item>
        </el-form>
        <div slot="footer" class="dialog-footer">
            <el-button @click="updateFormVisible = false">취소</el-button>
            <el-button type="primary" @click="update">확인</el-button>
        </div>
    </el-dialog>

</div>

</template>

<script>
    import Vue from 'vue';
    import VueResource from 'vue-resource';
    import VueMoment from 'vue-moment';
    var moment = require('moment-timezone');

    Vue.use(VueResource);
    Vue.use(VueMoment, moment);

    Vue.http.options.emulateJSON = true;
    const http=Vue.http;
   
    export default {
        name: "test",
        data: function() {
          return {
            tableData: [],

            url:'job/queryjob',

            pagesize: 10,

            currentPage: 1,

            start: 1,

            totalCount: 1000,

            dialogFormVisible: false,

            updateFormVisible: false,
            dialogFormVisibleChange: false,

            checkboxChange:false,

            form: {
                jobName: '',
                jobGroup: '',
                cronExpression: '',
                timeType: ''
            },
            jobs: [{value: 'insaUpdateJob', label: '서울시_인사정보업데이트'}, 
                   {value: 'insaUpdateCsvJobOfSeoulCity', label: '서울시_차세대업데이트'}, 
                   {value: 'insaTransferJobOfSeoulCity', label: '서울시_인사정보전송'},
                   {value: 'insaTransferJobOfMobile', label: '서울시_인사정보전송_모바일'}, 
                   {value: 'insaUpdateJobBmsv', label: '자치구_인사정보업데이트'},
                   {value: 'insaUpdateCsvJobOfBmsv', label: '자치구_차세대업데이트'},
                   {value: 'insaTransferJobOfBmsv', label: '자치구_인사정보전송'},
                   {value: 'insaTransferJobOfMobileBmsv', label: '자치구_인사정보전송_모바일'}    
                  ],
            ruleForm: {
                resource: 6
            },

            updateform: {
                jobName: '',
                jobGroup: '',
                cronExpression: '',
                lastSuccessTime: ''
            },
            options: [{
                value: 1,
                label: '연'
            }, {
                value: 2,
                label: '월'
            }, {
                value: 3,
                label: '일'
            }, {
                value: 4,
                label: '시'
            }, {
                value: 5,
                label: '분'
            }, {
                value: 6,
                label: '주'
            },{
              value: 7,
              label: '초'
             }],
            value4: '',
          };
        },
        created() {
		    // retrieve jobs
		    this.loadData(this.currentPage, this.pagesize);
	    },
        methods: {

            loadData: function(pageNum, pageSize){
                this.$http.get('job/queryjob?' + 'pageNum=' +  pageNum + '&pageSize=' + pageSize).then(function(res){
                    console.log(res)
                    this.tableData = res.body.JobAndTrigger.list;
                    this.totalCount = res.body.number;
                },function(){
                    console.log('failed');
                });
            },

            handleDelete: function(index, row) {
                this.$http.post('job/deletejob',{"jobClassName":row.job_NAME,"jobGroupName":row.job_GROUP},{emulateJSON: true}).then(function(res){
                    this.loadData( this.currentPage, this.pagesize);
                },function(){
                    console.log('failed');
                });
            },

            handlePause: function(index, row){
                this.$http.post('job/pausejob',{"jobClassName":row.job_NAME,"jobGroupName":row.job_GROUP},{emulateJSON: true}).then(function(res){
                    this.loadData( this.currentPage, this.pagesize);
                },function(){
                    console.log('failed');
                });
            },

            handleResume: function(index, row){
                this.$http.post('job/resumejob',{"jobClassName":row.job_NAME,"jobGroupName":row.job_GROUP},{emulateJSON: true}).then(function(res){
                    this.loadData( this.currentPage, this.pagesize);
                },function(){
                    console.log('failed');
                });
            },

            search: function(){
                this.loadData(this.currentPage, this.pagesize);
            },

            handleadd: function(){
                this.checkboxChange = true;
            },
            change: function(){
                this.dialogFormVisibleChange = true;
            },

            add: function(){
                this.$http.post('job/addjob',
                    {"jobClassName":this.form.jobName,
                     "jobGroupName":this.form.jobGroup,
                     "cronExpression":this.form.cronExpression},
                     {"Content-Type": "application/json"}).then(function(res){
                    this.loadData(this.currentPage, this.pagesize);
                    this.dialogFormVisibleChange = false;
                    this.checkboxChange = false;
                },function(){
                    console.log('failed');
                });
            },
            addSimTir: function () {
                console.log(this.value4)
                this.$http.post('job/addjob',
                    {"jobClassName":this.form.jobName,
                     "jobGroupName":this.form.jobGroup,
                     "cronExpression":this.form.cronExpression,
                     "timeType":this.value4},
                     {"Content-Type": "application/json"}).then(function(res){
                    this.loadData(this.currentPage, this.pagesize);
                    this.dialogFormVisibleChange = false;
                    this.checkboxChange = false;
                },function(){
                    console.log('failed');
                });
            },

            handleUpdate: function(index, row){
                console.log(row)
                this.updateFormVisible = true;
                this.updateform.jobName = row.job_NAME;//row.job_CLASS_NAME;
                this.updateform.jobGroup = row.job_GROUP;
                this.updateform.cronExpression = row.cron_EXPRESSION;
                this.updateform.lastSuccessTime = this.getDateString(row.last_SUCCESS_TIME);
            },

            update: function(){
                this.$http.post
                ('job/reschedulejob',
                    {"jobClassName":this.updateform.jobName,
                        "jobGroupName":this.updateform.jobGroup,
                        "cronExpression":this.updateform.cronExpression,
                        "lastSuccessTime":this.updateform.lastSuccessTime
                    },{emulateJSON: true}
                ).then(function(res){
                    this.loadData(this.currentPage, this.pagesize);
                    this.updateFormVisible = false;
                },function(){
                    console.log('failed');
                });

            },

            handleSizeChange: function(val) {
                this.pagesize = val;
                this.loadData(this.currentPage, this.pagesize);
            },

            handleCurrentChange: function(val) {
                this.currentPage = val;
                this.loadData(this.currentPage, this.pagesize);
            },
            
            dateFormat: function(row, column) {
                var dateVal = row[column.property];
                if (dateVal == undefined) { 
                    return "";
                }
                return this.$moment(dateVal).format('YYYY-MM-DD HH:mm:ss');
            },
            
            getDateString: function(dateVal) {
                if (dateVal == undefined) { 
                    return "";
                }
                return this.$moment(dateVal).format('YYYY-MM-DD HH:mm:ss');
            },

        },
    }
</script>

<style>
#top {
  background:#a6a2a2;
  padding:5px;
  overflow:hidden
}
.button{
	display:inline-block;
	color:#636262;
	background:white;
	border:1px solid #636262;
	border-radius: 3px; 
	width:80px;
	text-align:center;
	text-decoration:none;
	}
.button:hover{
	background:white;
	color:#3c55ba;
	}
</style>