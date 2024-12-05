const socket = io("http://localhost:5000");



socket.on("liquidation_update", (data) => {
    const tableBody = document.querySelector("#liquidation-table tbody");

    const newRow = `
        <tr>
            <td>${data.Symbol}</td>
            <td>${data.Side}</td>
            <td>${data.Price.toFixed(2)}</td>
            <td>${data.Quantity.toFixed(2)}</td>
            <td>${data["Total($)"].toFixed(2)}</td>
            <td>${data["Trade Time"]}</td>
        </tr>
    `;
    tableBody.insertAdjacentHTML("afterbegin", newRow);

    // Optional: Limit rows to the last 50 entries
    if (tableBody.rows.length > 50) {
        tableBody.deleteRow(-1);
    }
});
