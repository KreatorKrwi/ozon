async function loadOrder() {
    const orderId = document.getElementById("orderId").value.trim();
    if (!orderId) {
        alert("Введите ID заказа!");
        return;
    }

    try {
        const response = await fetch(`/order/${orderId}`);
        if (!response.ok) {
            throw new Error("Заказ не найден");
        }
        const order = await response.json();
        displayOrder(order);
    } catch (error) {
        document.getElementById("result").innerHTML = `
            <div class="error">Ошибка: ${error.message}</div>
        `;
    }
}

function displayOrder(order) {
    const resultDiv = document.getElementById("result");
    
    // Формируем HTML для товаров
    let itemsHtml = "<p>Нет товаров в заказе</p>";
    if (order.items && order.items.length > 0) {
        itemsHtml = `
            <h3>Товары:</h3>
            <ul>
                ${order.items.map(item => `
                    <li>${item.name} (${item.price} руб.)</li>
                `).join("")}
            </ul>
        `;
    }

    resultDiv.innerHTML = `
        <div class="order-info">
            <h2>Заказ #${order.order_uid}</h2>
            <p><strong>Трек-номер:</strong> ${order.track_number}</p>
            <p><strong>Статус:</strong> ${order.status || "не указан"}</p>
            ${itemsHtml}
        </div>
    `;
}