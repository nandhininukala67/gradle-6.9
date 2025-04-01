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
