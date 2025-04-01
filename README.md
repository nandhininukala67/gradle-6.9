public class PayoutTxnProcessService extends BaseThreadBasedPollingService {

    private static final int MAX_THREAD_COUNT = 30;  // Max number of threads
    private static final int TXNS_PER_THREAD = 1000;  // Number of transactions per thread

    @Override
    public String toString() {
        return super.toString() + "~" + getName();
    }

    public void process() throws InterruptedException {
        InstituteCache instituteCache = InstituteCacheHelper.getInstitute(institute);
        try {
            int offset = 0;
            int totalRemainingRecords = getTotalRemainingRecords();  // Fetch the total count of records

            while (offset < totalRemainingRecords) {
                int remainingRecords = totalRemainingRecords - offset;
                int threadsNeeded = Math.min(MAX_THREAD_COUNT, (int) Math.ceil((double) remainingRecords / TXNS_PER_THREAD));
                int batchSize = threadsNeeded * TXNS_PER_THREAD;

                // Fetch the next batch of transactions based on calculated offset and batch size
                List<MerchantPayoutDetails> payoutTxnList = fetchTransactions(offset, batchSize);
                offset += payoutTxnList.size();  // Update offset dynamically based on actual records fetched

                if (payoutTxnList.isEmpty()) {
                    getLog().info("No more payout transactions to process.");
                    break;  // Exit loop if no more records
                }

                logPayoutSummary(payoutTxnList);  // Log payout details
                getLog().info("Payout Txn Details Size: " + payoutTxnList.size());

                CountDownLatch latch = new CountDownLatch(payoutTxnList.size());

                // Process each payout transaction in a separate thread
                payoutTxnList.forEach(payoutDetails -> 
                    getThreadPoolExecutor().execute(new MerchantPayoutFeature(payoutDetails, instituteCache, transactionName, transactionManagerQueue, latch, getLog())));

                latch.await();  // Wait for all threads to finish processing the current batch
                getLog().info("Batch processed successfully.");
            }

            getLog().info("### All transactions are processed.");

        } catch (Exception e) {
            getLog().error("Error in processing the transaction: " + ExceptionUtils.getStackTrace(e));
        }
    }

    // Method to get total number of records to be processed
    private int getTotalRemainingRecords() throws Exception {
        // Return the total count of records that need processing
        return org.jpos.ee.DB.exec(db ->
            new MerchantPayoutManager(db).getTotalPendingTransactionCount(new SarvatraDateUtil().getStartOfDay()));
    }

    // Method to fetch a batch of transactions based on offset and batch size
    private List<MerchantPayoutDetails> fetchTransactions(int offset, int batchSize) throws Exception {
        // Fetch transactions with proper pagination (using offset and batchSize)
        return org.jpos.ee.DB.exec(db ->
            new MerchantPayoutManager(db).getMerchantPayoutTransactionList(new SarvatraDateUtil().getStartOfDay(), offset, batchSize));
    }

    // Method to log payout transaction summary
    private void logPayoutSummary(List<MerchantPayoutDetails> payoutTxnList) {
        LogEvent evt = getLog().createLogEvent("Merchant Payout Summary");
        evt.addMessage(LOG_SEPARATOR);
        evt.addMessage("Start Date        | " + new SarvatraDateUtil().getStartOfDay());
        evt.addMessage("Payout count      | " + payoutTxnList.size());
        evt.addMessage("Transaction Name  | " + this.transactionName);
        evt.addMessage(LOG_SEPARATOR);
        Logger.log(evt);
    }
}




package com.sarvatra.rtsp.service;

import com.sarvatra.common.util.SarvatraDateUtil;
import com.sarvatra.rtsp.cache.InstituteCache;
import com.sarvatra.rtsp.ee.MerchantPayoutDetails;
import com.sarvatra.rtsp.manager.MerchantPayoutManager;
import com.sarvatra.rtsp.server.InstituteCacheHelper;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jpos.util.LogEvent;
import org.jpos.util.Logger;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class PayoutTxnProcessService extends BaseThreadBasedPollingService {

    private static final int Max Thread=30;
    private static final int TxnPerThraed=1000;

    @Override
    public String toString() {
        return super.toString() + "~" + getName();
    }

    public void process() throws InterruptedException {
        InstituteCache instituteCache = InstituteCacheHelper.getInstitute(institute);
        try {

            getLog().info("######## getting payout transaction list");
            List<MerchantPayoutDetails> payoutTxnList = org.jpos.ee.DB.exec(db ->
                    new MerchantPayoutManager(db).getMerchantPayoutTransactionList(new SarvatraDateUtil().getStartOfDay()));

             logPayoutSummary(payoutTxnList);

            if (payoutTxnList.isEmpty()) {
                getLog().info("No payout transactions found");
                return;
            }

            // Queue payout transactions
            CountDownLatch latch = new CountDownLatch(payoutTxnList.size());
            getLog().info("Payout Txn Details Size :"+payoutTxnList.size());
            payoutTxnList.forEach(payoutDetails -> getThreadPoolExecutor().execute(new MerchantPayoutFeature(payoutDetails, instituteCache, transactionName, transactionManagerQueue, latch, getLog())));

            latch.await();
            getLog().info("Done with merchant payout service");

        } catch (Exception ex) {
            getLog().info("Exception occurred: " + ExceptionUtils.getStackTrace(ex));
        }
    }


    private void logPayoutSummary(List<MerchantPayoutDetails> payoutTxnList) {
        LogEvent evt = getLog().createLogEvent("Merchant Payout Summary");
        evt.addMessage(LOG_SEPARATOR);
        evt.addMessage("Start Date        | " + new SarvatraDateUtil().getStartOfDay());
        evt.addMessage("Payout count      | " + payoutTxnList.size());
        evt.addMessage("Transaction Name  | " + this.transactionName);
        evt.addMessage(LOG_SEPARATOR);
        Logger.log(evt);
    }
}

